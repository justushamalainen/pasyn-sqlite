//! Client for connecting to the writer server via Unix socket
//!
//! This module provides a client that sends write operations to the writer server.

use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use crate::error::{Error, ErrorCode, Result};
use crate::protocol::{read_message, write_message, Request, Response};
use crate::value::Value;

/// Client for sending write operations to the writer server
pub struct WriterClient {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
}

impl WriterClient {
    /// Connect to the writer server at the given socket path
    pub fn connect(socket_path: impl AsRef<Path>) -> io::Result<Self> {
        let stream = UnixStream::connect(socket_path)?;
        Self::from_stream(stream)
    }

    /// Connect with a timeout
    pub fn connect_timeout(socket_path: impl AsRef<Path>, timeout: Duration) -> io::Result<Self> {
        let stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Self::from_stream(stream)
    }

    fn from_stream(stream: UnixStream) -> io::Result<Self> {
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        Ok(WriterClient { reader, writer })
    }

    /// Send a request and receive a response
    fn send_request(&mut self, request: Request) -> Result<Response> {
        let data = request.serialize();
        write_message(&mut self.writer, &data).map_err(|e| {
            Error::with_message(ErrorCode::IoError, format!("Failed to send request: {}", e))
        })?;

        let response_data = read_message(&mut self.reader).map_err(|e| {
            Error::with_message(ErrorCode::IoError, format!("Failed to read response: {}", e))
        })?;

        Response::deserialize(&response_data).map_err(|e| {
            Error::with_message(
                ErrorCode::IoError,
                format!("Failed to deserialize response: {}", e),
            )
        })
    }

    /// Execute a SQL statement
    ///
    /// Returns the number of rows affected.
    pub fn execute<P: IntoIterator>(&mut self, sql: &str, params: P) -> Result<usize>
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute a SQL statement and return the last insert rowid
    pub fn execute_returning_rowid<P: IntoIterator>(&mut self, sql: &str, params: P) -> Result<i64>
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Execute multiple SQL statements (batch)
    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let request = Request::execute_batch(sql);
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Begin a transaction
    pub fn begin_transaction(&mut self) -> Result<()> {
        let request = Request::begin_transaction();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Commit the current transaction
    pub fn commit(&mut self) -> Result<()> {
        let request = Request::commit();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Rollback the current transaction
    pub fn rollback(&mut self) -> Result<()> {
        let request = Request::rollback();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Ping the server to check if it's alive
    pub fn ping(&mut self) -> Result<()> {
        let request = Request::ping();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }

    /// Request the server to shutdown
    pub fn shutdown(&mut self) -> Result<()> {
        let request = Request::shutdown();
        let response = self.send_request(request)?;

        if response.is_ok() {
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
    }
}

/// A hybrid connection that reads locally and writes via the writer server
///
/// This connection:
/// - Opens a local read-only SQLite connection for queries
/// - Sends all write operations to the writer server via Unix socket
pub struct HybridConnection {
    /// Local read-only connection
    read_conn: crate::connection::Connection,
    /// Client for write operations
    write_client: WriterClient,
}

impl HybridConnection {
    /// Create a new hybrid connection
    ///
    /// - `database_path`: Path to the SQLite database
    /// - `socket_path`: Path to the writer server Unix socket
    pub fn new(
        database_path: impl AsRef<Path>,
        socket_path: impl AsRef<Path>,
    ) -> Result<Self> {
        // Open read-only connection
        let read_conn = crate::connection::Connection::open_with_flags(
            database_path,
            crate::connection::OpenFlags::READONLY,
        )?;

        // Configure read connection
        read_conn.busy_timeout(5000)?;

        // Connect to writer server
        let write_client = WriterClient::connect(socket_path).map_err(|e| {
            Error::with_message(
                ErrorCode::CantOpen,
                format!("Failed to connect to writer server: {}", e),
            )
        })?;

        Ok(HybridConnection {
            read_conn,
            write_client,
        })
    }

    /// Get a reference to the read connection for queries
    pub fn read_connection(&self) -> &crate::connection::Connection {
        &self.read_conn
    }

    /// Execute a write operation via the writer server
    pub fn execute<P: IntoIterator>(&mut self, sql: &str, params: P) -> Result<usize>
    where
        P::Item: Into<Value>,
    {
        self.write_client.execute(sql, params)
    }

    /// Execute a write operation and return the last insert rowid
    pub fn execute_returning_rowid<P: IntoIterator>(&mut self, sql: &str, params: P) -> Result<i64>
    where
        P::Item: Into<Value>,
    {
        self.write_client.execute_returning_rowid(sql, params)
    }

    /// Execute a batch of SQL statements via the writer server
    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.write_client.execute_batch(sql)
    }

    /// Prepare a statement for reading
    pub fn prepare(&self, sql: &str) -> Result<crate::statement::Statement> {
        self.read_conn.prepare(sql)
    }

    /// Query and get a single row
    pub fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<Option<T>>
    where
        P: IntoIterator,
        P::Item: Into<Value>,
        F: FnOnce(&crate::connection::Row) -> Result<T>,
    {
        self.read_conn.query_row(sql, params, f)
    }

    /// Begin a transaction (on the writer server)
    pub fn begin_transaction(&mut self) -> Result<()> {
        self.write_client.begin_transaction()
    }

    /// Commit the current transaction
    pub fn commit(&mut self) -> Result<()> {
        self.write_client.commit()
    }

    /// Rollback the current transaction
    pub fn rollback(&mut self) -> Result<()> {
        self.write_client.rollback()
    }

    /// Ping the writer server
    pub fn ping(&mut self) -> Result<()> {
        self.write_client.ping()
    }
}

// =============================================================================
// Multiplexed Client - lock-free request submission with batched I/O
// =============================================================================

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
                response.error_message.unwrap_or_else(|| "Unknown error".to_string()),
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
        self.sender.send((id, request, response_tx)).map_err(|_| {
            Error::with_message(ErrorCode::IoError, "Channel closed")
        })?;

        // 2. Try to become the I/O handler
        if let Ok(mut io) = self.io.try_lock() {
            self.do_io(&mut io)?;
        }
        // If try_lock fails, another thread is handling I/O - they'll process our request

        // 3. Wait for our response
        response_rx.recv().map_err(|_| {
            Error::with_message(ErrorCode::IoError, "Response channel closed")
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::WriterServer;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_client_server_integration() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let socket_path = temp_dir.path().join("test.sock");

        // Start server in background
        let server = WriterServer::new(&db_path, &socket_path).unwrap();
        let handle = server.spawn().unwrap();

        // Wait for server to start
        thread::sleep(Duration::from_millis(100));

        // Connect client
        let mut client = WriterClient::connect(&socket_path).unwrap();

        // Ping server
        client.ping().unwrap();

        // Create table
        client.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap();

        // Insert data
        let rowid = client.execute_returning_rowid(
            "INSERT INTO test (name) VALUES (?)",
            ["Alice"],
        ).unwrap();
        assert_eq!(rowid, 1);

        // Shutdown server
        client.shutdown().unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_hybrid_connection() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let socket_path = temp_dir.path().join("test.sock");

        // Create initial database
        {
            let conn = crate::connection::Connection::open(&db_path).unwrap();
            conn.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)").unwrap();
        }

        // Start server in background
        let server = WriterServer::new(&db_path, &socket_path).unwrap();
        let handle = server.spawn().unwrap();

        // Wait for server to start
        thread::sleep(Duration::from_millis(100));

        // Create hybrid connection
        let mut hybrid = HybridConnection::new(&db_path, &socket_path).unwrap();

        // Write via server
        hybrid.execute("INSERT INTO test (value) VALUES (?)", ["hello"]).unwrap();

        // Wait for WAL to sync
        thread::sleep(Duration::from_millis(50));

        // Read locally
        let value: Option<String> = hybrid
            .query_row("SELECT value FROM test WHERE id = 1", std::iter::empty::<Value>(), |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, Some("hello".to_string()));

        // Cleanup
        drop(hybrid);
        WriterClient::connect(&socket_path).unwrap().shutdown().unwrap();
        handle.join().unwrap();
    }
}
