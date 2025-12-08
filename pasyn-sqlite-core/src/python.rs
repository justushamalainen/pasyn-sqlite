//! Python bindings for pasyn-sqlite-core using PyO3
//!
//! This module provides Python bindings to the SQLite functionality.

use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::connection::Connection as RustConnection;
use crate::connection::OpenFlags as RustOpenFlags;
use crate::error::Error as RustError;
use crate::server::{ServerConfig, ServerHandle, WriterServer};
use crate::client::MultiplexedClient as RustMultiplexedClient;
use crate::value::Value as RustValue;

use std::sync::Arc;
use std::path::PathBuf;

// Custom exception for SQLite errors
pyo3::create_exception!(pasyn_sqlite_core, SqliteError, PyException);

/// Convert a Rust error to a Python exception
fn to_py_err(e: RustError) -> PyErr {
    SqliteError::new_err(e.to_string())
}

/// Convert a Python value to a Rust Value
fn py_to_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RustValue> {
    if obj.is_none() {
        Ok(RustValue::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(RustValue::Integer(if b { 1 } else { 0 }))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(RustValue::Integer(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(RustValue::Real(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(RustValue::Text(s))
    } else if let Ok(bytes) = obj.downcast::<PyBytes>() {
        Ok(RustValue::Blob(bytes.as_bytes().to_vec()))
    } else {
        Err(PyValueError::new_err(format!(
            "Cannot convert {} to SQLite value",
            obj.get_type().name()?
        )))
    }
}

/// Convert a Rust Value to a Python object
fn value_to_py(py: Python<'_>, value: RustValue) -> PyObject {
    match value {
        RustValue::Null => py.None(),
        RustValue::Integer(i) => i.to_object(py),
        RustValue::Real(f) => f.to_object(py),
        RustValue::Text(s) => s.to_object(py),
        RustValue::Blob(b) => PyBytes::new_bound(py, &b).into_any().unbind(),
    }
}

/// Extract parameters from Python args
fn extract_params(py: Python<'_>, params: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<RustValue>> {
    match params {
        None => Ok(Vec::new()),
        Some(obj) => {
            if let Ok(list) = obj.downcast::<PyList>() {
                list.iter()
                    .map(|item| py_to_value(py, &item))
                    .collect()
            } else if let Ok(tuple) = obj.downcast::<PyTuple>() {
                tuple
                    .iter()
                    .map(|item| py_to_value(py, &item))
                    .collect()
            } else if let Ok(dict) = obj.downcast::<PyDict>() {
                // Named parameters - convert to positional for now
                // TODO: Support named parameters properly
                dict.values()
                    .iter()
                    .map(|item| py_to_value(py, &item))
                    .collect()
            } else {
                // Single value
                Ok(vec![py_to_value(py, obj)?])
            }
        }
    }
}

/// Open flags for database connections
#[pyclass(name = "OpenFlags")]
#[derive(Clone, Copy)]
pub struct PyOpenFlags {
    flags: i32,
}

#[pymethods]
impl PyOpenFlags {
    /// Create read-only flags
    #[staticmethod]
    fn readonly() -> Self {
        PyOpenFlags {
            flags: RustOpenFlags::READONLY.bits(),
        }
    }

    /// Create read-write flags
    #[staticmethod]
    fn readwrite() -> Self {
        PyOpenFlags {
            flags: RustOpenFlags::READWRITE.bits(),
        }
    }

    /// Create read-write with create flags
    #[staticmethod]
    fn create() -> Self {
        PyOpenFlags {
            flags: (RustOpenFlags::READWRITE | RustOpenFlags::CREATE).bits(),
        }
    }

    /// Combine with URI flag
    fn uri(&self) -> Self {
        PyOpenFlags {
            flags: self.flags | RustOpenFlags::URI.bits(),
        }
    }

    /// Combine with memory flag
    fn memory(&self) -> Self {
        PyOpenFlags {
            flags: self.flags | RustOpenFlags::MEMORY.bits(),
        }
    }

    /// Combine with shared cache flag
    fn shared_cache(&self) -> Self {
        PyOpenFlags {
            flags: self.flags | RustOpenFlags::SHAREDCACHE.bits(),
        }
    }

    fn __or__(&self, other: &PyOpenFlags) -> Self {
        PyOpenFlags {
            flags: self.flags | other.flags,
        }
    }
}

/// A SQLite database connection
/// No Mutex needed - SQLite handles synchronization internally (THREADSAFE=1)
#[pyclass(name = "Connection")]
pub struct PyConnection {
    conn: Arc<RustConnection>,
}

#[pymethods]
impl PyConnection {
    /// Open a database connection
    #[new]
    #[pyo3(signature = (path, flags=None))]
    fn new(path: &str, flags: Option<PyOpenFlags>) -> PyResult<Self> {
        let flags = flags.map(|f| RustOpenFlags::from_bits(f.flags)).unwrap_or_default();
        let conn = RustConnection::open_with_flags(path, flags).map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(conn),
        })
    }

    /// Open an in-memory database
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        let conn = RustConnection::open_in_memory().map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(conn),
        })
    }

    /// Open a shared in-memory database
    #[staticmethod]
    fn shared_memory(name: &str) -> PyResult<Self> {
        let conn = RustConnection::open_shared_memory(name).map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(conn),
        })
    }

    /// Execute a SQL statement
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<usize> {
        let params = extract_params(py, params)?;
        self.conn.execute(sql, params).map_err(to_py_err)
    }

    /// Execute multiple SQL statements
    fn executescript(&self, sql: &str) -> PyResult<()> {
        self.conn.execute_batch(sql).map_err(to_py_err)
    }

    /// Execute SQL and return all rows
    #[pyo3(signature = (sql, params=None))]
    fn execute_fetchall<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyList>> {
        let params = extract_params(py, params)?;
        let mut stmt = self.conn.query(sql, params).map_err(to_py_err)?;

        let mut rows = Vec::new();
        while stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            rows.push(PyTuple::new_bound(py, &row_values).into_any().unbind());
        }

        Ok(PyList::new_bound(py, &rows).unbind())
    }

    /// Execute SQL and return the first row
    #[pyo3(signature = (sql, params=None))]
    fn execute_fetchone<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Option<Py<PyTuple>>> {
        let params = extract_params(py, params)?;
        let mut stmt = self.conn.query(sql, params).map_err(to_py_err)?;

        if stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            Ok(Some(PyTuple::new_bound(py, &row_values).unbind()))
        } else {
            Ok(None)
        }
    }

    /// Create a cursor for iterating over results
    #[pyo3(signature = (sql, params=None))]
    fn cursor<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<PyCursor> {
        let params = extract_params(py, params)?;
        let stmt = self.conn.prepare(sql).map_err(to_py_err)?;
        let column_names: Vec<String> = (0..stmt.column_count())
            .filter_map(|i| stmt.column_name(i).map(String::from))
            .collect();

        Ok(PyCursor {
            conn: self.conn.clone(),
            sql: sql.to_string(),
            params,
            column_names,
            executed: false,
        })
    }

    /// Begin a transaction
    fn begin(&self) -> PyResult<()> {
        self.conn.execute_batch("BEGIN").map_err(to_py_err)
    }

    /// Commit the current transaction
    fn commit(&self) -> PyResult<()> {
        self.conn.execute_batch("COMMIT").map_err(to_py_err)
    }

    /// Rollback the current transaction
    fn rollback(&self) -> PyResult<()> {
        self.conn.execute_batch("ROLLBACK").map_err(to_py_err)
    }

    /// Check if in autocommit mode
    #[getter]
    fn in_transaction(&self) -> bool {
        !self.conn.is_autocommit()
    }

    /// Get the last inserted row ID
    #[getter]
    fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    /// Get the number of rows changed
    #[getter]
    fn changes(&self) -> i64 {
        self.conn.changes()
    }

    /// Get total changes since connection opened
    #[getter]
    fn total_changes(&self) -> i64 {
        self.conn.total_changes()
    }

    /// Set busy timeout in milliseconds
    fn set_busy_timeout(&self, ms: i32) -> PyResult<()> {
        self.conn.busy_timeout(ms).map_err(to_py_err)
    }

    /// Interrupt any pending operation
    fn interrupt(&self) {
        self.conn.interrupt();
    }

    /// Close the connection
    fn close(&self) -> PyResult<()> {
        // The actual close happens when the Arc is dropped
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// A database cursor for iterating over query results
/// No Mutex needed - SQLite handles synchronization internally (THREADSAFE=1)
#[pyclass(name = "Cursor")]
pub struct PyCursor {
    conn: Arc<RustConnection>,
    sql: String,
    params: Vec<RustValue>,
    column_names: Vec<String>,
    executed: bool,
}

#[pymethods]
impl PyCursor {
    /// Execute the query
    fn execute(&mut self) -> PyResult<()> {
        self.executed = true;
        Ok(())
    }

    /// Fetch all remaining rows
    fn fetchall<'py>(&mut self, py: Python<'py>) -> PyResult<Py<PyList>> {
        if !self.executed {
            self.execute()?;
        }

        let mut stmt = self.conn.query(&self.sql, self.params.clone()).map_err(to_py_err)?;

        let mut rows = Vec::new();
        while stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            rows.push(PyTuple::new_bound(py, &row_values).into_any().unbind());
        }

        Ok(PyList::new_bound(py, &rows).unbind())
    }

    /// Fetch the next row
    fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Py<PyTuple>>> {
        if !self.executed {
            self.execute()?;
        }

        let mut stmt = self.conn.query(&self.sql, self.params.clone()).map_err(to_py_err)?;

        if stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            Ok(Some(PyTuple::new_bound(py, &row_values).unbind()))
        } else {
            Ok(None)
        }
    }

    /// Fetch many rows
    #[pyo3(signature = (size=None))]
    fn fetchmany<'py>(&mut self, py: Python<'py>, size: Option<usize>) -> PyResult<Py<PyList>> {
        let size = size.unwrap_or(100);

        if !self.executed {
            self.execute()?;
        }

        let mut stmt = self.conn.query(&self.sql, self.params.clone()).map_err(to_py_err)?;

        let mut rows = Vec::new();
        let mut count = 0;
        while count < size && stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            rows.push(PyTuple::new_bound(py, &row_values).into_any().unbind());
            count += 1;
        }

        Ok(PyList::new_bound(py, &rows).unbind())
    }

    /// Get column names
    #[getter]
    fn description<'py>(&self, py: Python<'py>) -> PyResult<Py<PyList>> {
        let desc: Vec<Py<PyTuple>> = self
            .column_names
            .iter()
            .map(|name| {
                Ok(PyTuple::new_bound(
                    py,
                    &[
                        name.clone().to_object(py),
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                    ],
                ).unbind())
            })
            .collect::<PyResult<_>>()?;

        Ok(PyList::new_bound(py, &desc).unbind())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Get SQLite version string
#[pyfunction]
fn sqlite_version() -> &'static str {
    crate::sqlite_version()
}

/// Get SQLite version number
#[pyfunction]
fn sqlite_version_number() -> i32 {
    crate::sqlite_version_number()
}

/// Check if SQLite is thread-safe
#[pyfunction]
fn sqlite_threadsafe() -> bool {
    crate::sqlite_threadsafe()
}

/// Get memory currently used by SQLite
#[pyfunction]
fn memory_used() -> i64 {
    crate::memory_used()
}

/// Get memory high-water mark
#[pyfunction]
#[pyo3(signature = (reset=false))]
fn memory_highwater(reset: bool) -> i64 {
    crate::memory_highwater(reset)
}

/// Connect to a database (convenience function)
#[pyfunction]
#[pyo3(signature = (path, flags=None))]
fn connect(path: &str, flags: Option<PyOpenFlags>) -> PyResult<PyConnection> {
    PyConnection::new(path, flags)
}

// =============================================================================
// Writer Server bindings
// =============================================================================

/// Handle to a running writer server
#[pyclass(name = "WriterServerHandle")]
pub struct PyWriterServerHandle {
    handle: Option<ServerHandle>,
    socket_path: PathBuf,
}

#[pymethods]
impl PyWriterServerHandle {
    /// Get the socket path
    #[getter]
    fn socket_path(&self) -> String {
        self.socket_path.to_string_lossy().to_string()
    }

    /// Signal the server to shutdown
    fn shutdown(&self) -> PyResult<()> {
        if let Some(ref handle) = self.handle {
            handle.shutdown();
        }
        Ok(())
    }

    /// Wait for the server to stop
    fn join(&mut self) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to join server: {}", e))
            })?;
        }
        Ok(())
    }

    /// Shutdown and wait for the server to stop
    fn stop(&mut self) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            handle.stop().map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to stop server: {}", e))
            })?;
        }
        Ok(())
    }
}

/// Start a writer server that handles all write operations via Unix socket
#[pyfunction]
#[pyo3(signature = (database_path, socket_path=None))]
fn start_writer_server(database_path: &str, socket_path: Option<&str>) -> PyResult<PyWriterServerHandle> {
    let socket_path = socket_path
        .map(PathBuf::from)
        .unwrap_or_else(|| ServerConfig::default_socket_path(database_path));

    let server = WriterServer::new(database_path, &socket_path).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to create server: {}", e))
    })?;

    let handle = server.spawn().map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to start server: {}", e))
    })?;

    // Give the server a moment to start
    std::thread::sleep(std::time::Duration::from_millis(50));

    Ok(PyWriterServerHandle {
        handle: Some(handle),
        socket_path,
    })
}

/// Get the default socket path for a database
#[pyfunction]
fn default_socket_path(database_path: &str) -> String {
    ServerConfig::default_socket_path(database_path).to_string_lossy().to_string()
}

// =============================================================================
// Protocol serialization for async I/O
// These functions allow Python to handle async socket I/O natively
// =============================================================================

use crate::protocol::{Request, Response};

/// Serialize an execute request to bytes
#[pyfunction]
#[pyo3(signature = (sql, params=None))]
fn serialize_execute_request<'py>(
    py: Python<'py>,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
) -> PyResult<Py<PyBytes>> {
    let params = extract_params(py, params)?;
    let request = Request::execute(sql, params);
    let data = request.serialize();
    // Prepend length (4 bytes, little-endian)
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    Ok(PyBytes::new_bound(py, &msg).unbind())
}

/// Serialize an execute_returning_rowid request to bytes
#[pyfunction]
#[pyo3(signature = (sql, params=None))]
fn serialize_execute_returning_rowid_request<'py>(
    py: Python<'py>,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
) -> PyResult<Py<PyBytes>> {
    let params = extract_params(py, params)?;
    let request = Request::execute_returning_rowid(sql, params);
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    Ok(PyBytes::new_bound(py, &msg).unbind())
}

/// Serialize an execute_batch request to bytes
#[pyfunction]
fn serialize_executescript_request(py: Python<'_>, sql: &str) -> Py<PyBytes> {
    let request = Request::execute_batch(sql);
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    PyBytes::new_bound(py, &msg).unbind()
}

/// Serialize a begin transaction request to bytes
#[pyfunction]
fn serialize_begin_request(py: Python<'_>) -> Py<PyBytes> {
    let request = Request::begin_transaction();
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    PyBytes::new_bound(py, &msg).unbind()
}

/// Serialize a commit request to bytes
#[pyfunction]
fn serialize_commit_request(py: Python<'_>) -> Py<PyBytes> {
    let request = Request::commit();
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    PyBytes::new_bound(py, &msg).unbind()
}

/// Serialize a rollback request to bytes
#[pyfunction]
fn serialize_rollback_request(py: Python<'_>) -> Py<PyBytes> {
    let request = Request::rollback();
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    PyBytes::new_bound(py, &msg).unbind()
}

/// Serialize a ping request to bytes
#[pyfunction]
fn serialize_ping_request(py: Python<'_>) -> Py<PyBytes> {
    let request = Request::ping();
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    PyBytes::new_bound(py, &msg).unbind()
}

/// Serialize a shutdown request to bytes
#[pyfunction]
fn serialize_shutdown_request(py: Python<'_>) -> Py<PyBytes> {
    let request = Request::shutdown();
    let data = request.serialize();
    let mut msg = (data.len() as u32).to_le_bytes().to_vec();
    msg.extend(data);
    PyBytes::new_bound(py, &msg).unbind()
}

/// Parse response from bytes, returns (success, rows_affected, last_insert_rowid, error_message)
#[pyfunction]
fn parse_response(py: Python<'_>, data: &[u8]) -> PyResult<(bool, i64, i64, Option<String>)> {
    let response = Response::deserialize(data).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to parse response: {}", e))
    })?;
    Ok((
        response.is_ok(),
        response.rows_affected,
        response.last_insert_rowid,
        response.error_message,
    ))
}

// =============================================================================
// Multiplexed Client - thread-safe with automatic batching
// =============================================================================

/// A multiplexed client that allows concurrent request submission from multiple threads.
///
/// This client uses a lock-free channel for request submission and automatically
/// batches requests for efficient I/O. Multiple threads can call execute() concurrently
/// without blocking each other (except briefly during actual I/O).
///
/// Features:
/// - Thread-safe: can be shared across Python threads
/// - Automatic batching: requests are batched together for efficiency
/// - Lock-free submission: requests are submitted to a channel without blocking
/// - No background threads: I/O is handled by one of the calling threads
#[pyclass(name = "MultiplexedClient")]
pub struct PyMultiplexedClient {
    client: Arc<RustMultiplexedClient>,
}

#[pymethods]
impl PyMultiplexedClient {
    /// Create a new multiplexed client connected to the given socket path.
    #[new]
    fn new(socket_path: &str) -> PyResult<Self> {
        let client = RustMultiplexedClient::connect(socket_path).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to connect: {}", e))
        })?;
        Ok(PyMultiplexedClient {
            client: Arc::new(client),
        })
    }

    /// Execute a SQL statement. Returns the number of rows affected.
    ///
    /// This method is thread-safe and can be called from multiple threads concurrently.
    /// The GIL is released during I/O operations.
    #[pyo3(signature = (sql, params=None))]
    fn execute(&self, py: Python<'_>, sql: String, params: Option<&Bound<'_, PyAny>>) -> PyResult<i64> {
        let params = extract_params(py, params)?;
        let client = self.client.clone();

        // Release GIL during blocking I/O
        py.allow_threads(move || {
            client.execute(&sql, params).map(|rows| rows as i64).map_err(to_py_err)
        })
    }

    /// Execute a SQL statement and return the last insert rowid.
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid(&self, py: Python<'_>, sql: String, params: Option<&Bound<'_, PyAny>>) -> PyResult<i64> {
        let params = extract_params(py, params)?;
        let client = self.client.clone();

        py.allow_threads(move || {
            client.execute_returning_rowid(&sql, params).map_err(to_py_err)
        })
    }

    /// Execute the same SQL statement with multiple parameter sets.
    ///
    /// This is more efficient than calling execute() multiple times because
    /// the statement is prepared once and reused for each parameter set.
    /// Returns the total number of rows affected.
    fn execute_many(&self, py: Python<'_>, sql: String, params_batch: &Bound<'_, PyList>) -> PyResult<i64> {
        // Convert Python list of parameter lists to Vec<Vec<Value>>
        let batch: Vec<Vec<RustValue>> = params_batch
            .iter()
            .map(|item| extract_params(py, Some(&item)))
            .collect::<PyResult<_>>()?;

        let client = self.client.clone();

        py.allow_threads(move || {
            client.execute_many(&sql, batch).map(|rows| rows as i64).map_err(to_py_err)
        })
    }

    /// Execute multiple SQL statements (batch/script).
    fn executescript(&self, py: Python<'_>, sql: String) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || {
            client.execute_batch(&sql).map_err(to_py_err)
        })
    }

    /// Begin a transaction.
    fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || {
            client.begin_transaction().map_err(to_py_err)
        })
    }

    /// Commit the current transaction.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || {
            client.commit().map_err(to_py_err)
        })
    }

    /// Rollback the current transaction.
    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || {
            client.rollback().map_err(to_py_err)
        })
    }

    /// Ping the server to check if it's alive.
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || {
            client.ping().map_err(to_py_err)
        })
    }

    /// Request the server to shutdown.
    fn shutdown_server(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || {
            client.shutdown().map_err(to_py_err)
        })
    }
}

/// Create a multiplexed client (convenience function).
#[pyfunction]
fn multiplexed_client(socket_path: &str) -> PyResult<PyMultiplexedClient> {
    PyMultiplexedClient::new(socket_path)
}

/// Python module
#[pymodule]
fn pasyn_sqlite_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Connection classes
    m.add_class::<PyConnection>()?;
    m.add_class::<PyCursor>()?;
    m.add_class::<PyOpenFlags>()?;

    // Writer server class
    m.add_class::<PyWriterServerHandle>()?;

    // Multiplexed client class
    m.add_class::<PyMultiplexedClient>()?;

    // Connection functions
    m.add_function(wrap_pyfunction!(connect, m)?)?;

    // Server functions
    m.add_function(wrap_pyfunction!(start_writer_server, m)?)?;
    m.add_function(wrap_pyfunction!(default_socket_path, m)?)?;
    m.add_function(wrap_pyfunction!(multiplexed_client, m)?)?;

    // Protocol serialization functions (for async I/O)
    m.add_function(wrap_pyfunction!(serialize_execute_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_execute_returning_rowid_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_executescript_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_begin_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_commit_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_rollback_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_ping_request, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_shutdown_request, m)?)?;
    m.add_function(wrap_pyfunction!(parse_response, m)?)?;

    // Utility functions
    m.add_function(wrap_pyfunction!(sqlite_version, m)?)?;
    m.add_function(wrap_pyfunction!(sqlite_version_number, m)?)?;
    m.add_function(wrap_pyfunction!(sqlite_threadsafe, m)?)?;
    m.add_function(wrap_pyfunction!(memory_used, m)?)?;
    m.add_function(wrap_pyfunction!(memory_highwater, m)?)?;

    // Exceptions
    m.add("SqliteError", m.py().get_type_bound::<SqliteError>())?;

    Ok(())
}
