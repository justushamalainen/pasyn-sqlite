//! Writer server that handles all write operations via Unix socket
//!
//! This module implements a single-writer server that:
//! - Listens on a Unix socket for write requests
//! - Maintains a single SQLite connection for writes
//! - Uses poll() to handle multiple clients concurrently
//! - **Batches concurrent writes** from ALL clients using savepoints

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::connection::Connection;
use crate::protocol::{Request, RequestType, Response};

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
    /// Maximum batch size for write batching (default: 100)
    pub max_batch_size: usize,
    /// Enable write batching with savepoints (default: true)
    pub enable_batching: bool,
}

impl ServerConfig {
    /// Create a new server config
    pub fn new(database_path: impl AsRef<Path>, socket_path: impl AsRef<Path>) -> Self {
        ServerConfig {
            database_path: database_path.as_ref().to_path_buf(),
            socket_path: socket_path.as_ref().to_path_buf(),
            busy_timeout_ms: 5000,
            wal_mode: true,
            max_batch_size: 100,
            enable_batching: true,
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

    /// Set maximum batch size for write batching
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Enable or disable write batching
    pub fn enable_batching(mut self, enable: bool) -> Self {
        self.enable_batching = enable;
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

/// Client connection state
struct ClientState {
    stream: UnixStream,
    read_buf: Vec<u8>,
    pending_requests: Vec<Request>,
}

impl ClientState {
    fn new(stream: UnixStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(ClientState {
            stream,
            read_buf: Vec::with_capacity(4096),
            pending_requests: Vec::new(),
        })
    }
}

/// A pending request with its source client ID
struct PendingRequest {
    client_id: usize,
    request: Request,
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
        listener.set_nonblocking(true)?;
        let listener_fd = listener.as_raw_fd();

        // Track client connections: id -> ClientState
        let mut clients: HashMap<usize, ClientState> = HashMap::new();
        let mut next_client_id = 1usize;

        println!(
            "Writer server started: socket={}, db={}",
            self.config.socket_path.display(),
            self.config.database_path.display()
        );

        // Main event loop
        while !self.is_shutdown() {
            // Build poll fds array: listener + all clients
            let mut pollfds: Vec<libc::pollfd> = Vec::with_capacity(1 + clients.len());

            // Add listener
            pollfds.push(libc::pollfd {
                fd: listener_fd,
                events: libc::POLLIN,
                revents: 0,
            });

            // Map from pollfd index to client id
            let mut pollfd_to_client: Vec<Option<usize>> = vec![None]; // index 0 is listener

            // Add all clients
            for (&client_id, client) in &clients {
                pollfds.push(libc::pollfd {
                    fd: client.stream.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                });
                pollfd_to_client.push(Some(client_id));
            }

            // Poll with 100ms timeout
            let nready = unsafe {
                libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, 100)
            };

            if nready < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }

            if nready == 0 {
                // Timeout - just continue to check shutdown flag
                continue;
            }

            // Collect all pending requests and track disconnected clients
            let mut all_pending: Vec<PendingRequest> = Vec::new();
            let mut clients_to_remove: Vec<usize> = Vec::new();

            // Check listener for new connections
            if pollfds[0].revents & libc::POLLIN != 0 {
                loop {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            match ClientState::new(stream) {
                                Ok(client) => {
                                    let client_id = next_client_id;
                                    next_client_id += 1;
                                    clients.insert(client_id, client);
                                }
                                Err(e) => {
                                    eprintln!("Error setting up client: {}", e);
                                }
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            eprintln!("Error accepting connection: {}", e);
                            break;
                        }
                    }
                }
            }

            // Check clients for data
            for (i, pollfd) in pollfds.iter().enumerate().skip(1) {
                if pollfd.revents == 0 {
                    continue;
                }

                let client_id = match pollfd_to_client[i] {
                    Some(id) => id,
                    None => continue,
                };

                // Check for errors or hangup
                if pollfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                    clients_to_remove.push(client_id);
                    continue;
                }

                // Read data
                if pollfd.revents & libc::POLLIN != 0 {
                    if let Some(client) = clients.get_mut(&client_id) {
                        match self.read_client_data(client) {
                            Ok(true) => {
                                // Parse requests
                                self.parse_requests(client);
                                // Collect pending requests
                                for request in client.pending_requests.drain(..) {
                                    all_pending.push(PendingRequest { client_id, request });
                                }
                            }
                            Ok(false) => {
                                // Client disconnected
                                clients_to_remove.push(client_id);
                            }
                            Err(e) => {
                                eprintln!("Error reading from client {}: {}", client_id, e);
                                clients_to_remove.push(client_id);
                            }
                        }
                    }
                }
            }

            // Remove disconnected clients
            for client_id in &clients_to_remove {
                clients.remove(client_id);
            }

            // Process all pending requests as a batch
            if !all_pending.is_empty() {
                let responses = self.process_batch_multi_client(&conn, &all_pending);

                // Send responses back to respective clients
                for (pending, response) in all_pending.iter().zip(responses.iter()) {
                    if let Some(client) = clients.get_mut(&pending.client_id) {
                        if let Err(e) = self.send_response(&mut client.stream, response) {
                            eprintln!("Error sending response to client {}: {}", pending.client_id, e);
                            // Don't remove client here - they might recover
                        }
                    }
                }

                // Check for shutdown requests
                for pending in &all_pending {
                    if pending.request.request_type == RequestType::Shutdown {
                        self.shutdown();
                        break;
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

    /// Read available data from a client (non-blocking)
    fn read_client_data(&self, client: &mut ClientState) -> io::Result<bool> {
        let mut buf = [0u8; 4096];
        loop {
            match client.stream.read(&mut buf) {
                Ok(0) => return Ok(false), // EOF - client disconnected
                Ok(n) => {
                    client.read_buf.extend_from_slice(&buf[..n]);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(true);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Parse complete requests from the client's read buffer
    fn parse_requests(&self, client: &mut ClientState) {
        loop {
            // Need at least 4 bytes for length prefix
            if client.read_buf.len() < 4 {
                break;
            }

            // Read length prefix (little-endian, same as protocol.rs)
            let len = u32::from_le_bytes([
                client.read_buf[0],
                client.read_buf[1],
                client.read_buf[2],
                client.read_buf[3],
            ]) as usize;

            // Check if we have the complete message
            if client.read_buf.len() < 4 + len {
                break;
            }

            // Extract and parse the request
            let request_data: Vec<u8> = client.read_buf.drain(..4 + len).skip(4).collect();
            match Request::deserialize(&request_data) {
                Ok(request) => {
                    client.pending_requests.push(request);
                }
                Err(e) => {
                    eprintln!("Error parsing request: {}", e);
                }
            }
        }
    }

    /// Send a response to a client (blocking write)
    fn send_response(&self, stream: &mut UnixStream, response: &Response) -> io::Result<()> {
        // Temporarily set blocking for write
        stream.set_nonblocking(false)?;

        let data = response.serialize();
        let len = data.len() as u32;

        // Write length prefix (little-endian, same as protocol.rs)
        stream.write_all(&len.to_le_bytes())?;
        // Write data
        stream.write_all(&data)?;
        stream.flush()?;

        // Restore non-blocking
        stream.set_nonblocking(true)?;
        Ok(())
    }

    /// Process a batch of requests from multiple clients
    fn process_batch_multi_client(
        &self,
        conn: &Connection,
        pending: &[PendingRequest],
    ) -> Vec<Response> {
        // If only one request or batching disabled, process individually
        if pending.len() == 1 || !self.config.enable_batching {
            return pending
                .iter()
                .map(|p| {
                    self.process_request(conn, &p.request)
                        .with_id(p.request.request_id)
                })
                .collect();
        }

        // Check if all requests are batchable
        let all_batchable = pending
            .iter()
            .all(|p| Self::is_batchable(p.request.request_type));

        if all_batchable && conn.is_autocommit() {
            // Batch with savepoints
            self.process_batch_with_savepoints_multi(conn, pending)
        } else {
            // Mixed batch - process individually
            pending
                .iter()
                .map(|p| {
                    self.process_request(conn, &p.request)
                        .with_id(p.request.request_id)
                })
                .collect()
        }
    }

    /// Process multiple requests from multiple clients in a single transaction
    fn process_batch_with_savepoints_multi(
        &self,
        conn: &Connection,
        pending: &[PendingRequest],
    ) -> Vec<Response> {
        let mut responses = Vec::with_capacity(pending.len());

        // Begin batch transaction
        if let Err(e) = conn.execute_batch("BEGIN") {
            return pending
                .iter()
                .map(|p| {
                    Response::error(format!("Batch begin failed: {}", e))
                        .with_id(p.request.request_id)
                })
                .collect();
        }

        // Process each request with a savepoint
        for (i, pending_req) in pending.iter().enumerate() {
            let sp_name = format!("sp{}", i);

            // Create savepoint
            if let Err(e) = conn.execute_batch(&format!("SAVEPOINT {}", sp_name)) {
                responses.push(
                    Response::error(format!("Savepoint failed: {}", e))
                        .with_id(pending_req.request.request_id),
                );
                continue;
            }

            // Process the request
            let response = self.process_single_in_savepoint(conn, &pending_req.request, &sp_name);
            responses.push(response.with_id(pending_req.request.request_id));
        }

        // Commit the batch transaction
        if let Err(e) = conn.execute_batch("COMMIT") {
            let _ = conn.execute_batch("ROLLBACK");
            for (i, response) in responses.iter_mut().enumerate() {
                if response.is_ok() {
                    *response = Response::error(format!("Batch commit failed: {}", e))
                        .with_id(pending[i].request.request_id);
                }
            }
        }

        responses
    }

    /// Check if a request type can be batched with savepoints
    fn is_batchable(request_type: RequestType) -> bool {
        matches!(
            request_type,
            RequestType::Execute | RequestType::ExecuteReturningRowId
        )
    }

    /// Process a single request within a savepoint
    fn process_single_in_savepoint(
        &self,
        conn: &Connection,
        request: &Request,
        sp_name: &str,
    ) -> Response {
        match request.request_type {
            RequestType::Execute | RequestType::ExecuteReturningRowId => {
                if let Some(ref sql) = request.sql {
                    match conn.execute(sql, request.params.clone()) {
                        Ok(rows) => {
                            let _ = conn.execute_batch(&format!("RELEASE {}", sp_name));
                            Response::ok(rows as i64, conn.last_insert_rowid())
                        }
                        Err(e) => {
                            let _ = conn.execute_batch(&format!("ROLLBACK TO {}", sp_name));
                            let _ = conn.execute_batch(&format!("RELEASE {}", sp_name));
                            Response::error(e.to_string())
                        }
                    }
                } else {
                    let _ = conn.execute_batch(&format!("RELEASE {}", sp_name));
                    Response::error("Missing SQL statement")
                }
            }
            _ => {
                let _ = conn.execute_batch(&format!("RELEASE {}", sp_name));
                self.process_request(conn, request)
            }
        }
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
                    if conn.is_autocommit() {
                        if let Err(e) = conn.execute_batch("BEGIN") {
                            return Response::error(e.to_string());
                        }
                        let result = conn.execute_many(sql, request.params_batch.clone());
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
            RequestType::Shutdown => Response::simple_ok(),
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
        // Connect to unblock the poll() call
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
