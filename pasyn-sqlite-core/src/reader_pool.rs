//! Read thread pool for handling concurrent read queries
//!
//! This module provides a pool of worker threads, each with its own SQLite connection,
//! for handling read queries in parallel. This bypasses Python's GIL by doing all
//! SQLite work in Rust threads.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::connection::Connection;
use crate::error::Result;
use crate::protocol::Response;
use crate::value::Value;

/// A query job to be executed by a worker thread
struct QueryJob {
    request_id: u64,
    sql: String,
    params: Vec<Value>,
    response_tx: Sender<Response>,
}

/// A worker thread with its own SQLite connection
struct Worker {
    handle: JoinHandle<()>,
}

/// Configuration for the reader pool
#[derive(Debug, Clone)]
pub struct ReaderPoolConfig {
    /// Path to the SQLite database
    pub database_path: PathBuf,
    /// Number of reader threads
    pub num_threads: usize,
    /// Busy timeout in milliseconds (default: 5000)
    pub busy_timeout_ms: i32,
}

impl ReaderPoolConfig {
    /// Create a new config with defaults
    pub fn new(database_path: impl AsRef<Path>, num_threads: usize) -> Self {
        ReaderPoolConfig {
            database_path: database_path.as_ref().to_path_buf(),
            num_threads,
            busy_timeout_ms: 5000,
        }
    }

    /// Set the busy timeout
    pub fn busy_timeout(mut self, ms: i32) -> Self {
        self.busy_timeout_ms = ms;
        self
    }
}

/// A pool of reader threads for executing queries in parallel
pub struct ReaderPool {
    job_tx: Sender<QueryJob>,
    workers: Vec<Worker>,
    shutdown: Arc<AtomicBool>,
    next_id: AtomicU64,
    num_threads: usize,
}

impl ReaderPool {
    /// Create a new reader pool
    pub fn new(config: ReaderPoolConfig) -> Result<Self> {
        let (job_tx, job_rx) = channel::<QueryJob>();
        let job_rx = Arc::new(std::sync::Mutex::new(job_rx));
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(config.num_threads);

        for i in 0..config.num_threads {
            let db_path = config.database_path.clone();
            let busy_timeout = config.busy_timeout_ms;
            let rx = Arc::clone(&job_rx);
            let shutdown_flag = Arc::clone(&shutdown);

            let handle = thread::Builder::new()
                .name(format!("reader-{}", i))
                .spawn(move || {
                    // Open read-only connection for this thread
                    let conn = match Connection::open_with_flags(
                        &db_path,
                        crate::connection::OpenFlags::READONLY,
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("Reader thread {} failed to open database: {}", i, e);
                            return;
                        }
                    };

                    // Configure connection
                    let _ = conn.busy_timeout(busy_timeout);

                    // Process jobs until shutdown
                    loop {
                        // Try to get a job
                        let job = {
                            let rx_guard = rx.lock().unwrap();
                            rx_guard.recv()
                        };

                        match job {
                            Ok(job) => {
                                let response = Self::execute_query(&conn, &job);
                                let _ = job.response_tx.send(response);
                            }
                            Err(_) => {
                                // Channel closed, exit
                                break;
                            }
                        }

                        if shutdown_flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                })
                .map_err(|e| {
                    crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("Failed to spawn reader thread: {}", e),
                    )
                })?;

            workers.push(Worker { handle });
        }

        Ok(ReaderPool {
            job_tx,
            workers,
            shutdown,
            next_id: AtomicU64::new(1),
            num_threads: config.num_threads,
        })
    }

    /// Execute a query and return the response
    fn execute_query(conn: &Connection, job: &QueryJob) -> Response {
        match conn.query(&job.sql, job.params.clone()) {
            Ok(mut stmt) => {
                // Get column names
                let mut column_names = Vec::new();
                for i in 0..stmt.column_count() {
                    if let Some(name) = stmt.column_name(i) {
                        column_names.push(name.to_string());
                    } else {
                        column_names.push(format!("column_{}", i));
                    }
                }

                // Fetch all rows
                let mut rows = Vec::new();
                loop {
                    match stmt.step() {
                        Ok(true) => {
                            let mut row = Vec::with_capacity(stmt.column_count());
                            for i in 0..stmt.column_count() {
                                row.push(stmt.column_value(i));
                            }
                            rows.push(row);
                        }
                        Ok(false) => break,
                        Err(e) => {
                            return Response::error(format!("Error fetching rows: {}", e))
                                .with_id(job.request_id);
                        }
                    }
                }

                Response::with_rows(column_names, rows).with_id(job.request_id)
            }
            Err(e) => Response::error(format!("Query error: {}", e)).with_id(job.request_id),
        }
    }

    /// Submit a query and get a receiver for the response
    pub fn query(&self, sql: &str, params: Vec<Value>) -> Receiver<Response> {
        let (response_tx, response_rx) = channel();
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let job = QueryJob {
            request_id,
            sql: sql.to_string(),
            params,
            response_tx,
        };

        // Send to worker pool
        let _ = self.job_tx.send(job);

        response_rx
    }

    /// Submit a query and block until complete (for sync usage)
    pub fn query_sync(&self, sql: &str, params: Vec<Value>) -> Response {
        let rx = self.query(sql, params);
        rx.recv().unwrap_or_else(|_| Response::error("Channel closed"))
    }

    /// Get number of reader threads
    pub fn num_threads(&self) -> usize {
        self.num_threads
    }

    /// Shutdown the pool
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl Drop for ReaderPool {
    fn drop(&mut self) {
        self.shutdown();
        // Drop the sender to close the channel, which will cause workers to exit
        // Workers will be joined when they're dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[test]
    fn test_reader_pool_basic() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        // Create and populate database
        {
            let conn = Connection::open(db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO test VALUES (1, 'Alice');
                 INSERT INTO test VALUES (2, 'Bob');",
            )
            .unwrap();
        }

        // Create reader pool
        let config = ReaderPoolConfig::new(db_path, 2);
        let pool = ReaderPool::new(config).unwrap();

        // Query
        let response = pool.query_sync("SELECT * FROM test ORDER BY id", vec![]);
        assert!(response.is_ok());
        assert_eq!(response.rows.len(), 2);
        assert_eq!(response.column_names, vec!["id", "name"]);
    }

    #[test]
    fn test_reader_pool_concurrent() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path();

        // Create and populate database
        {
            let conn = Connection::open(db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);",
            )
            .unwrap();
            for i in 0..100 {
                conn.execute("INSERT INTO test VALUES (?, ?)", vec![Value::Integer(i), Value::Integer(i * 10)]).unwrap();
            }
        }

        // Create reader pool with 4 threads
        let config = ReaderPoolConfig::new(db_path, 4);
        let pool = Arc::new(ReaderPool::new(config).unwrap());

        // Submit multiple concurrent queries
        let mut receivers = Vec::new();
        for i in 0..10 {
            let rx = pool.query(
                "SELECT * FROM test WHERE id = ?",
                vec![Value::Integer(i)],
            );
            receivers.push(rx);
        }

        // Collect results
        for rx in receivers {
            let response = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(response.is_ok());
            assert_eq!(response.rows.len(), 1);
        }
    }
}
