//! Client for connecting to the writer server via Unix socket
//!
//! This module provides a multiplexed client that sends write operations to the writer server.

use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use crate::error::{Error, ErrorCode, Result};
use crate::protocol::{read_message, write_message, Request, Response};
use crate::value::Value;

/// Internal state for I/O operations
struct IoState {
    receiver: Receiver<(u64, Request, Sender<Response>)>,
    writer: BufWriter<UnixStream>,
    reader: BufReader<UnixStream>,
    pending: HashMap<u64, Sender<Response>>,
}

/// A multiplexed client that allows concurrent request submission.
///
/// This client uses a lock-free channel for request submission and a single
/// mutex for I/O. When multiple tasks submit requests concurrently:
/// - All tasks can submit to the channel without blocking (lock-free)
/// - One task becomes the "I/O handler" and processes requests for everyone
/// - Other tasks wait on their response channel
///
/// This design provides:
/// - Automatic request batching (multiple requests sent together)
/// - No lock contention on submission
/// - No background threads required
pub struct MultiplexedClient {
    sender: Sender<(u64, Request, Sender<Response>)>,
    io: Mutex<IoState>,
    next_id: AtomicU64,
}

impl MultiplexedClient {
    /// Connect to the writer server at the given socket path
    pub fn connect(socket_path: impl AsRef<Path>) -> io::Result<Self> {
        let stream = UnixStream::connect(socket_path)?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);

        let (sender, receiver) = channel();

        Ok(MultiplexedClient {
            sender,
            io: Mutex::new(IoState {
                receiver,
                writer,
                reader,
                pending: HashMap::new(),
            }),
            next_id: AtomicU64::new(1),
        })
    }

    /// Execute a SQL statement. Returns the number of rows affected.
    pub fn execute<P: IntoIterator>(&self, sql: &str, params: P) -> Result<usize>
    where
        P::Item: Into<Value>,
    {
        let params: Vec<Value> = params.into_iter().map(|p| p.into()).collect();
        let request = Request::execute(sql, params);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(response.rows_affected as usize)
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute a SQL statement and return the last insert rowid
    pub fn execute_returning_rowid<P: IntoIterator>(&self, sql: &str, params: P) -> Result<i64>
    where
        P::Item: Into<Value>,
    {
        let params: Vec<Value> = params.into_iter().map(|p| p.into()).collect();
        let request = Request::execute_returning_rowid(sql, params);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(response.last_insert_rowid)
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute multiple SQL statements (batch)
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let request = Request::execute_batch(sql);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute the same SQL with multiple parameter sets (executemany)
    ///
    /// Returns the total number of rows affected.
    pub fn execute_many(&self, sql: &str, params_batch: Vec<Vec<Value>>) -> Result<usize> {
        let request = Request::execute_many(sql, params_batch);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(response.rows_affected as usize)
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Begin a transaction
    pub fn begin_transaction(&self) -> Result<()> {
        let request = Request::begin_transaction();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Commit the current transaction
    pub fn commit(&self) -> Result<()> {
        let request = Request::commit();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Rollback the current transaction
    pub fn rollback(&self) -> Result<()> {
        let request = Request::rollback();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Ping the server
    pub fn ping(&self) -> Result<()> {
        let request = Request::ping();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Request the server to shutdown
    pub fn shutdown(&self) -> Result<()> {
        let request = Request::shutdown();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Acquire an exclusive transaction lock.
    ///
    /// This returns a Transaction guard that holds the lock. While the lock is held,
    /// no other clients can perform write operations. The transaction must be
    /// committed or rolled back within the server's timeout period (default: 1 second).
    ///
    /// The Transaction guard implements Drop, so if it goes out of scope without
    /// being committed, it will automatically rollback.
    pub fn begin_exclusive(&self) -> Result<Transaction> {
        let request = Request::acquire_transaction_lock();
        let response = self.send_request(request)?;

        if response.is_ok() {
            let token = response.transaction_token.ok_or_else(|| {
                Error::with_message(ErrorCode::Error, "No transaction token received")
            })?;
            Ok(Transaction {
                client: self,
                token,
                finished: false,
            })
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Failed to acquire transaction lock".to_string()),
            ))
        }
    }

    /// Execute within a transaction (with token)
    pub fn execute_with_token<P: IntoIterator>(
        &self,
        sql: &str,
        params: P,
        token: u64,
    ) -> Result<usize>
    where
        P::Item: Into<Value>,
    {
        let params: Vec<Value> = params.into_iter().map(|p| p.into()).collect();
        let request = Request::execute(sql, params).with_token(token);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(response.rows_affected as usize)
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute returning rowid within a transaction (with token)
    pub fn execute_returning_rowid_with_token<P: IntoIterator>(
        &self,
        sql: &str,
        params: P,
        token: u64,
    ) -> Result<i64>
    where
        P::Item: Into<Value>,
    {
        let params: Vec<Value> = params.into_iter().map(|p| p.into()).collect();
        let request = Request::execute_returning_rowid(sql, params).with_token(token);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(response.last_insert_rowid)
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute batch within a transaction (with token)
    pub fn execute_batch_with_token(&self, sql: &str, token: u64) -> Result<()> {
        let request = Request::execute_batch(sql).with_token(token);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Commit with token
    pub fn commit_with_token(&self, token: u64) -> Result<()> {
        let request = Request::commit().with_token(token);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Rollback with token
    pub fn rollback_with_token(&self, token: u64) -> Result<()> {
        let request = Request::rollback().with_token(token);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Send a request and wait for the response.
    ///
    /// This is the core multiplexing logic:
    /// 1. Assign a unique ID and create a response channel
    /// 2. Submit to the lock-free queue
    /// 3. Try to become the I/O handler (non-blocking try_lock)
    /// 4. If we got the lock: process all pending requests
    /// 5. Wait for our response on the channel
    fn send_request(&self, request: Request) -> Result<Response> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = request.with_id(id);
        let (response_tx, response_rx) = channel();

        // 1. Submit to channel (lock-free, never blocks)
        self.sender
            .send((id, request, response_tx))
            .map_err(|_| Error::with_message(ErrorCode::IoError, "Channel closed"))?;

        // 2. Try to become the I/O handler
        if let Ok(mut io) = self.io.try_lock() {
            self.do_io(&mut io)?;
        }
        // If try_lock fails, another thread is handling I/O - they'll process our request

        // 3. Wait for our response
        response_rx
            .recv()
            .map_err(|_| Error::with_message(ErrorCode::IoError, "Response channel closed"))
    }

    /// Process all pending I/O. Called while holding the io lock.
    fn do_io(&self, io: &mut IoState) -> Result<()> {
        use std::io::Write;

        loop {
            // Drain all pending requests from the channel
            while let Ok((id, request, response_tx)) = io.receiver.try_recv() {
                // Serialize and send
                let data = request.serialize();
                write_message(&mut io.writer, &data).map_err(|e| {
                    Error::with_message(ErrorCode::IoError, format!("Failed to send: {}", e))
                })?;
                io.pending.insert(id, response_tx);
            }

            // If nothing pending, we're done
            if io.pending.is_empty() {
                break;
            }

            // Flush all writes
            io.writer.flush().map_err(|e| {
                Error::with_message(ErrorCode::IoError, format!("Failed to flush: {}", e))
            })?;

            // Read responses until all pending are satisfied
            while !io.pending.is_empty() {
                let response_data = read_message(&mut io.reader).map_err(|e| {
                    Error::with_message(ErrorCode::IoError, format!("Failed to read: {}", e))
                })?;

                let response = Response::deserialize(&response_data).map_err(|e| {
                    Error::with_message(ErrorCode::IoError, format!("Failed to deserialize: {}", e))
                })?;

                // Dispatch to the waiting task by request_id
                if let Some(tx) = io.pending.remove(&response.request_id) {
                    let _ = tx.send(response);
                }
            }

            // Check if more requests arrived while we were reading
            // (loop continues if there are new requests)
        }

        Ok(())
    }
}

/// An exclusive transaction guard.
///
/// This struct represents an exclusive transaction lock. While this guard exists,
/// no other clients can perform write operations on the database.
///
/// The transaction must be explicitly committed with `commit()`. If the guard
/// is dropped without committing, the transaction will be automatically rolled back.
///
/// # Example
///
/// ```no_run
/// use pasyn_sqlite_core::client::MultiplexedClient;
/// use pasyn_sqlite_core::value::Value;
///
/// let client = MultiplexedClient::connect("/tmp/db.sock").unwrap();
/// let tx = client.begin_exclusive().unwrap();
///
/// tx.execute("INSERT INTO users VALUES (?, ?)", [Value::Integer(1), Value::Text("Alice".to_string())]).unwrap();
/// tx.execute("INSERT INTO users VALUES (?, ?)", [Value::Integer(2), Value::Text("Bob".to_string())]).unwrap();
///
/// tx.commit().unwrap();  // Commits the transaction and releases the lock
/// ```
pub struct Transaction<'a> {
    client: &'a MultiplexedClient,
    token: u64,
    finished: bool,
}

impl<'a> Transaction<'a> {
    /// Get the transaction token (for debugging/logging)
    pub fn token(&self) -> u64 {
        self.token
    }

    /// Execute a SQL statement within this transaction.
    pub fn execute<P: IntoIterator>(&self, sql: &str, params: P) -> Result<usize>
    where
        P::Item: Into<Value>,
    {
        self.client.execute_with_token(sql, params, self.token)
    }

    /// Execute a SQL statement and return the last insert rowid.
    pub fn execute_returning_rowid<P: IntoIterator>(&self, sql: &str, params: P) -> Result<i64>
    where
        P::Item: Into<Value>,
    {
        self.client
            .execute_returning_rowid_with_token(sql, params, self.token)
    }

    /// Execute multiple SQL statements (batch/script) within this transaction.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.client.execute_batch_with_token(sql, self.token)
    }

    /// Commit the transaction and release the lock.
    ///
    /// After this call, the transaction guard is consumed and the lock is released.
    pub fn commit(mut self) -> Result<()> {
        self.finished = true;
        self.client.commit_with_token(self.token)
    }

    /// Rollback the transaction and release the lock.
    ///
    /// After this call, the transaction guard is consumed and the lock is released.
    pub fn rollback(mut self) -> Result<()> {
        self.finished = true;
        self.client.rollback_with_token(self.token)
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Auto-rollback on drop (panic safety)
            let _ = self.client.rollback_with_token(self.token);
        }
    }
}
