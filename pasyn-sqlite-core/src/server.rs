//! Writer server that handles all write operations via Unix socket
//!
//! This module implements a single-writer server that:
//! - Listens on a Unix socket for write requests
//! - Maintains a single SQLite connection for writes
//! - Serializes all write operations
//!
//! Usage:
//! ```no_run
//! use pasyn_sqlite_core::server::WriterServer;
//!
//! let server = WriterServer::new("/path/to/db.sqlite", "/tmp/pasyn-writer.sock")?;
//! server.run()?; // Blocks until shutdown
//! # Ok::<(), std::io::Error>(())
//! ```

use std::fs;
use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::connection::Connection;
use crate::protocol::{read_message, write_message, Request, RequestType, Response};

/// Configuration for the writer server
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Path to the SQLite database
    pub database_path: PathBuf,
    /// Path for the Unix socket
    pub socket_path: PathBuf,
    /// Busy timeout in milliseconds (default: 5000)
    pub busy_timeout_ms: i32,
    /// Enable WAL mode (default: true)
    pub wal_mode: bool,
}

impl ServerConfig {
    /// Create a new server config
    pub fn new(database_path: impl AsRef<Path>, socket_path: impl AsRef<Path>) -> Self {
        ServerConfig {
            database_path: database_path.as_ref().to_path_buf(),
            socket_path: socket_path.as_ref().to_path_buf(),
            busy_timeout_ms: 5000,
            wal_mode: true,
        }
    }

    /// Set the busy timeout
    pub fn busy_timeout(mut self, ms: i32) -> Self {
        self.busy_timeout_ms = ms;
        self
    }

    /// Set whether to enable WAL mode
    pub fn wal_mode(mut self, enable: bool) -> Self {
        self.wal_mode = enable;
        self
    }

    /// Generate a default socket path based on the database path
    pub fn default_socket_path(database_path: impl AsRef<Path>) -> PathBuf {
        let db_path = database_path.as_ref();
        let file_name = db_path.file_name().unwrap_or_default().to_string_lossy();
        let socket_name = format!(".{}.writer.sock", file_name);

        if let Some(parent) = db_path.parent() {
            parent.join(socket_name)
        } else {
            PathBuf::from(socket_name)
        }
    }
}

/// Writer server that handles all SQLite write operations
pub struct WriterServer {
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
}

impl WriterServer {
    /// Create a new writer server
    pub fn new(database_path: impl AsRef<Path>, socket_path: impl AsRef<Path>) -> io::Result<Self> {
        let config = ServerConfig::new(database_path, socket_path);
        Ok(WriterServer {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Create a server with custom configuration
    pub fn with_config(config: ServerConfig) -> io::Result<Self> {
        Ok(WriterServer {
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get the socket path
    pub fn socket_path(&self) -> &Path {
        &self.config.socket_path
    }

    /// Signal the server to shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if the server is shutting down
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Run the server (blocking)
    pub fn run(&self) -> io::Result<()> {
        // Remove existing socket file if it exists
        if self.config.socket_path.exists() {
            fs::remove_file(&self.config.socket_path)?;
        }

        // Create the socket directory if it doesn't exist
        if let Some(parent) = self.config.socket_path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Open the SQLite connection
        let conn = Connection::open(&self.config.database_path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to open database: {}", e),
            )
        })?;

        // Configure the connection
        conn.busy_timeout(self.config.busy_timeout_ms)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to set busy timeout: {}", e),
                )
            })?;

        if self.config.wal_mode {
            conn.execute_batch("PRAGMA journal_mode=WAL").map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to enable WAL mode: {}", e),
                )
            })?;
        }

        // Create the Unix socket listener
        let listener = UnixListener::bind(&self.config.socket_path)?;

        // Set non-blocking so we can check for shutdown
        listener.set_nonblocking(true)?;

        println!(
            "Writer server started: socket={}, db={}",
            self.config.socket_path.display(),
            self.config.database_path.display()
        );

        // Accept connections
        while !self.is_shutdown() {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    stream.set_nonblocking(false)?;
                    if let Err(e) = self.handle_connection(&conn, stream) {
                        eprintln!("Error handling connection: {}", e);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No connection available, sleep briefly and try again
                    thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => {
                    if !self.is_shutdown() {
                        eprintln!("Error accepting connection: {}", e);
                    }
                }
            }
        }

        // Cleanup
        drop(conn);
        let _ = fs::remove_file(&self.config.socket_path);

        println!("Writer server stopped");
        Ok(())
    }

    /// Handle a single client connection
    fn handle_connection(&self, conn: &Connection, stream: UnixStream) -> io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream);

        loop {
            // Read request
            let data = match read_message(&mut reader) {
                Ok(data) => data,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Client disconnected
                    break;
                }
                Err(e) => return Err(e),
            };

            let request = Request::deserialize(&data)?;

            // Handle shutdown request
            if request.request_type == RequestType::Shutdown {
                let response = Response::simple_ok().with_id(request.request_id);
                write_message(&mut writer, &response.serialize())?;
                self.shutdown();
                break;
            }

            // Process request and echo back request_id
            let response = self
                .process_request(conn, &request)
                .with_id(request.request_id);

            // Send response
            write_message(&mut writer, &response.serialize())?;
        }

        Ok(())
    }

    /// Process a single request
    fn process_request(&self, conn: &Connection, request: &Request) -> Response {
        match request.request_type {
            RequestType::Execute => {
                if let Some(ref sql) = request.sql {
                    match conn.execute(sql, request.params.clone()) {
                        Ok(rows) => Response::ok(rows as i64, conn.last_insert_rowid()),
                        Err(e) => Response::error(e.to_string()),
                    }
                } else {
                    Response::error("Missing SQL statement")
                }
            }
            RequestType::ExecuteReturningRowId => {
                if let Some(ref sql) = request.sql {
                    match conn.execute(sql, request.params.clone()) {
                        Ok(rows) => Response::ok(rows as i64, conn.last_insert_rowid()),
                        Err(e) => Response::error(e.to_string()),
                    }
                } else {
                    Response::error("Missing SQL statement")
                }
            }
            RequestType::ExecuteMany => {
                if let Some(ref sql) = request.sql {
                    // Wrap in transaction for efficiency (prevents auto-commit per row)
                    if conn.is_autocommit() {
                        // Start implicit transaction
                        if let Err(e) = conn.execute_batch("BEGIN") {
                            return Response::error(e.to_string());
                        }
                        let result = conn.execute_many(sql, request.params_batch.clone());
                        // Commit the transaction
                        if let Err(e) = conn.execute_batch("COMMIT") {
                            let _ = conn.execute_batch("ROLLBACK");
                            return Response::error(e.to_string());
                        }
                        match result {
                            Ok(rows) => Response::ok(rows as i64, conn.last_insert_rowid()),
                            Err(e) => {
                                let _ = conn.execute_batch("ROLLBACK");
                                Response::error(e.to_string())
                            }
                        }
                    } else {
                        // Already in a transaction, just execute
                        match conn.execute_many(sql, request.params_batch.clone()) {
                            Ok(rows) => Response::ok(rows as i64, conn.last_insert_rowid()),
                            Err(e) => Response::error(e.to_string()),
                        }
                    }
                } else {
                    Response::error("Missing SQL statement")
                }
            }
            RequestType::ExecuteBatch => {
                if let Some(ref sql) = request.sql {
                    match conn.execute_batch(sql) {
                        Ok(()) => Response::simple_ok(),
                        Err(e) => Response::error(e.to_string()),
                    }
                } else {
                    Response::error("Missing SQL statement")
                }
            }
            RequestType::BeginTransaction => match conn.execute_batch("BEGIN") {
                Ok(()) => Response::simple_ok(),
                Err(e) => Response::error(e.to_string()),
            },
            RequestType::Commit => match conn.execute_batch("COMMIT") {
                Ok(()) => Response::simple_ok(),
                Err(e) => Response::error(e.to_string()),
            },
            RequestType::Rollback => match conn.execute_batch("ROLLBACK") {
                Ok(()) => Response::simple_ok(),
                Err(e) => Response::error(e.to_string()),
            },
            RequestType::Ping => Response::simple_ok(),
            RequestType::Shutdown => {
                // Handled in handle_connection
                Response::simple_ok()
            }
        }
    }

    /// Start the server in a background thread
    pub fn spawn(self) -> io::Result<ServerHandle> {
        let shutdown = self.shutdown.clone();
        let socket_path = self.config.socket_path.clone();

        let handle = thread::Builder::new()
            .name("pasyn-writer-server".to_string())
            .spawn(move || {
                if let Err(e) = self.run() {
                    eprintln!("Writer server error: {}", e);
                }
            })?;

        Ok(ServerHandle {
            thread: Some(handle),
            shutdown,
            socket_path,
        })
    }
}

/// Handle to a running writer server
pub struct ServerHandle {
    thread: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    socket_path: PathBuf,
}

impl ServerHandle {
    /// Get the socket path
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Signal the server to shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Connect to unblock the accept() call
        let _ = UnixStream::connect(&self.socket_path);
    }

    /// Wait for the server to stop
    pub fn join(mut self) -> io::Result<()> {
        if let Some(handle) = self.thread.take() {
            handle
                .join()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Server thread panicked"))?;
        }
        Ok(())
    }

    /// Shutdown and wait for the server to stop
    pub fn stop(self) -> io::Result<()> {
        self.shutdown();
        self.join()
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
        // Don't wait for join in drop to avoid blocking
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_default_socket_path() {
        let socket_path = ServerConfig::default_socket_path("/path/to/mydb.sqlite");
        assert_eq!(
            socket_path,
            PathBuf::from("/path/to/.mydb.sqlite.writer.sock")
        );
    }

    #[test]
    fn test_server_config() {
        let config = ServerConfig::new("/tmp/test.db", "/tmp/test.sock")
            .busy_timeout(10000)
            .wal_mode(false);

        assert_eq!(config.database_path, PathBuf::from("/tmp/test.db"));
        assert_eq!(config.socket_path, PathBuf::from("/tmp/test.sock"));
        assert_eq!(config.busy_timeout_ms, 10000);
        assert!(!config.wal_mode);
    }
}
