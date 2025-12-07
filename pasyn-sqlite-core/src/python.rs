//! Python bindings for pasyn-sqlite-core using PyO3
//!
//! This module provides Python bindings to the SQLite functionality.

use pyo3::exceptions::{PyException, PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::connection::Connection as RustConnection;
use crate::connection::OpenFlags as RustOpenFlags;
use crate::error::Error as RustError;
use crate::statement::Statement as RustStatement;
use crate::value::Value as RustValue;
use crate::server::{ServerConfig, ServerHandle, WriterServer};
use crate::client::{HybridConnection as RustHybridConnection, WriterClient as RustWriterClient};

use std::sync::{Arc, Mutex};
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
#[pyclass(name = "Connection")]
pub struct PyConnection {
    conn: Arc<Mutex<RustConnection>>,
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
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory database
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        let conn = RustConnection::open_in_memory().map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open a shared in-memory database
    #[staticmethod]
    fn shared_memory(name: &str) -> PyResult<Self> {
        let conn = RustConnection::open_shared_memory(name).map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(Mutex::new(conn)),
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
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.execute(sql, params).map_err(to_py_err)
    }

    /// Execute multiple SQL statements
    fn executescript(&self, sql: &str) -> PyResult<()> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.execute_batch(sql).map_err(to_py_err)
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
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let mut stmt = conn.query(sql, params).map_err(to_py_err)?;

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
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let mut stmt = conn.query(sql, params).map_err(to_py_err)?;

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
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;

        let stmt = conn.prepare(sql).map_err(to_py_err)?;
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
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.execute_batch("BEGIN").map_err(to_py_err)
    }

    /// Commit the current transaction
    fn commit(&self) -> PyResult<()> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.execute_batch("COMMIT").map_err(to_py_err)
    }

    /// Rollback the current transaction
    fn rollback(&self) -> PyResult<()> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.execute_batch("ROLLBACK").map_err(to_py_err)
    }

    /// Check if in autocommit mode
    #[getter]
    fn in_transaction(&self) -> PyResult<bool> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        Ok(!conn.is_autocommit())
    }

    /// Get the last inserted row ID
    #[getter]
    fn last_insert_rowid(&self) -> PyResult<i64> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        Ok(conn.last_insert_rowid())
    }

    /// Get the number of rows changed
    #[getter]
    fn changes(&self) -> PyResult<i64> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        Ok(conn.changes())
    }

    /// Get total changes since connection opened
    #[getter]
    fn total_changes(&self) -> PyResult<i64> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        Ok(conn.total_changes())
    }

    /// Set busy timeout in milliseconds
    fn set_busy_timeout(&self, ms: i32) -> PyResult<()> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.busy_timeout(ms).map_err(to_py_err)
    }

    /// Interrupt any pending operation
    fn interrupt(&self) -> PyResult<()> {
        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        conn.interrupt();
        Ok(())
    }

    /// Close the connection
    fn close(&self) -> PyResult<()> {
        // The actual close happens when the Arc is dropped
        // For now, just verify we can get the lock
        let _conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
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
#[pyclass(name = "Cursor")]
pub struct PyCursor {
    conn: Arc<Mutex<RustConnection>>,
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

        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let mut stmt = conn.query(&self.sql, self.params.clone()).map_err(to_py_err)?;

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

        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let mut stmt = conn.query(&self.sql, self.params.clone()).map_err(to_py_err)?;

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

        let conn = self.conn.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let mut stmt = conn.query(&self.sql, self.params.clone()).map_err(to_py_err)?;

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
// Writer Server and Client bindings
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

use crate::protocol::{Request, Response, RequestType};

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

/// Client for sending write operations to the writer server
#[pyclass(name = "WriterClient")]
pub struct PyWriterClient {
    client: Mutex<RustWriterClient>,
}

#[pymethods]
impl PyWriterClient {
    /// Connect to a writer server
    #[new]
    fn new(socket_path: &str) -> PyResult<Self> {
        let client = RustWriterClient::connect(socket_path).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to connect to writer server: {}", e))
        })?;
        Ok(PyWriterClient {
            client: Mutex::new(client),
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
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.execute(sql, params).map_err(to_py_err)
    }

    /// Execute a SQL statement and return the last insert rowid
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<i64> {
        let params = extract_params(py, params)?;
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.execute_returning_rowid(sql, params).map_err(to_py_err)
    }

    /// Execute multiple SQL statements (batch)
    fn executescript(&self, sql: &str) -> PyResult<()> {
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.execute_batch(sql).map_err(to_py_err)
    }

    /// Begin a transaction
    fn begin(&self) -> PyResult<()> {
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.begin_transaction().map_err(to_py_err)
    }

    /// Commit the current transaction
    fn commit(&self) -> PyResult<()> {
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.commit().map_err(to_py_err)
    }

    /// Rollback the current transaction
    fn rollback(&self) -> PyResult<()> {
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.rollback().map_err(to_py_err)
    }

    /// Ping the server
    fn ping(&self) -> PyResult<()> {
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.ping().map_err(to_py_err)
    }

    /// Shutdown the server
    fn shutdown_server(&self) -> PyResult<()> {
        let mut client = self.client.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        client.shutdown().map_err(to_py_err)
    }
}

/// A hybrid connection that reads locally and writes via the writer server
///
/// This is the recommended way to use the library for concurrent access:
/// - Read operations are performed directly on a local read-only connection
/// - Write operations are sent to the writer server via Unix socket
#[pyclass(name = "HybridConnection")]
pub struct PyHybridConnection {
    hybrid: Mutex<RustHybridConnection>,
}

#[pymethods]
impl PyHybridConnection {
    /// Create a new hybrid connection
    ///
    /// - `database_path`: Path to the SQLite database
    /// - `socket_path`: Path to the writer server Unix socket
    #[new]
    fn new(database_path: &str, socket_path: &str) -> PyResult<Self> {
        let hybrid = RustHybridConnection::new(database_path, socket_path).map_err(to_py_err)?;
        Ok(PyHybridConnection {
            hybrid: Mutex::new(hybrid),
        })
    }

    /// Execute a write operation via the writer server
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<usize> {
        let params = extract_params(py, params)?;
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.execute(sql, params).map_err(to_py_err)
    }

    /// Execute a write operation and return the last insert rowid
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<i64> {
        let params = extract_params(py, params)?;
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.execute_returning_rowid(sql, params).map_err(to_py_err)
    }

    /// Execute multiple SQL statements via the writer server
    fn executescript(&self, sql: &str) -> PyResult<()> {
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.execute_batch(sql).map_err(to_py_err)
    }

    /// Query data locally (read-only) and return all rows
    #[pyo3(signature = (sql, params=None))]
    fn query_fetchall<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyList>> {
        let params = extract_params(py, params)?;
        let hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let read_conn = hybrid.read_connection();
        let mut stmt = read_conn.query(sql, params).map_err(to_py_err)?;

        let mut rows = Vec::new();
        while stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            rows.push(PyTuple::new_bound(py, &row_values).into_any().unbind());
        }

        Ok(PyList::new_bound(py, &rows).unbind())
    }

    /// Query data locally and return the first row
    #[pyo3(signature = (sql, params=None))]
    fn query_fetchone<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Option<Py<PyTuple>>> {
        let params = extract_params(py, params)?;
        let hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let read_conn = hybrid.read_connection();
        let mut stmt = read_conn.query(sql, params).map_err(to_py_err)?;

        if stmt.step().map_err(to_py_err)? {
            let row_values: Vec<PyObject> = (0..stmt.column_count())
                .map(|i| value_to_py(py, stmt.column_value(i)))
                .collect();
            Ok(Some(PyTuple::new_bound(py, &row_values).unbind()))
        } else {
            Ok(None)
        }
    }

    /// Begin a transaction (on the writer server)
    fn begin(&self) -> PyResult<()> {
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.begin_transaction().map_err(to_py_err)
    }

    /// Commit the current transaction
    fn commit(&self) -> PyResult<()> {
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.commit().map_err(to_py_err)
    }

    /// Rollback the current transaction
    fn rollback(&self) -> PyResult<()> {
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.rollback().map_err(to_py_err)
    }

    /// Ping the writer server
    fn ping(&self) -> PyResult<()> {
        let mut hybrid = self.hybrid.lock().map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        hybrid.ping().map_err(to_py_err)
    }
}

/// Connect to a hybrid connection (convenience function)
#[pyfunction]
fn hybrid_connect(database_path: &str, socket_path: &str) -> PyResult<PyHybridConnection> {
    PyHybridConnection::new(database_path, socket_path)
}

// =============================================================================
// Native Awaitable Client - GIL-releasing async I/O
// =============================================================================

use std::os::unix::net::UnixStream;
use std::io::{BufReader, BufWriter};
use crate::protocol::{read_message, write_message};

/// Create a StopIteration exception with the given value.
/// This properly sets the `value` attribute, which is what Python's await extracts.
fn make_stop_iteration(py: Python<'_>, value: PyObject) -> PyErr {
    // Create StopIteration exception with the value as a single argument
    // This ensures StopIteration.value is set correctly
    let stop_iter = py.get_type_bound::<PyStopIteration>();
    let exc = stop_iter.call1((value,)).expect("Failed to create StopIteration");
    PyErr::from_value_bound(exc.into_any())
}

/// Native awaitable for execute operations.
///
/// This implements the Python awaitable protocol (__await__, __iter__, __next__)
/// and releases the GIL during blocking socket I/O.
#[pyclass]
pub struct NativeExecuteAwaitable {
    socket_path: String,
    request_bytes: Vec<u8>,
    completed: bool,
    result: Option<(bool, i64, i64, Option<String>)>,
}

#[pymethods]
impl NativeExecuteAwaitable {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if slf.completed {
            // Already completed - return cached result
            if let Some(result) = slf.result.take() {
                let result_obj = result.to_object(py);
                return Err(make_stop_iteration(py, result_obj));
            }
            return Err(PyStopIteration::new_err(py.None()));
        }

        // Release GIL and perform blocking socket I/O
        let socket_path = slf.socket_path.clone();
        let request_bytes = slf.request_bytes.clone();

        let result = py.allow_threads(move || {
            // Connect to socket
            let stream = UnixStream::connect(&socket_path)?;
            let mut reader = BufReader::new(stream.try_clone()?);
            let mut writer = BufWriter::new(stream);

            // Send request (request_bytes already includes length prefix)
            use std::io::Write;
            writer.write_all(&request_bytes)?;
            writer.flush()?;

            // Read response
            let response_data = read_message(&mut reader)?;
            let response = Response::deserialize(&response_data).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;

            Ok::<_, std::io::Error>((
                response.is_ok(),
                response.rows_affected,
                response.last_insert_rowid,
                response.error_message,
            ))
        });

        slf.completed = true;

        match result {
            Ok(result) => {
                slf.result = Some(result.clone());
                let result_obj = result.to_object(py);
                Err(make_stop_iteration(py, result_obj))
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Socket error: {}", e))),
        }
    }
}

/// Native awaitable client that releases GIL during I/O.
///
/// Each method returns a native awaitable that performs the socket I/O
/// with the GIL released, allowing other Python threads to run.
#[pyclass(name = "NativeAsyncClient")]
pub struct PyNativeAsyncClient {
    socket_path: String,
}

#[pymethods]
impl PyNativeAsyncClient {
    #[new]
    fn new(socket_path: &str) -> Self {
        PyNativeAsyncClient {
            socket_path: socket_path.to_string(),
        }
    }

    /// Execute a SQL statement asynchronously (returns awaitable).
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<NativeExecuteAwaitable> {
        let params = extract_params(py, params)?;
        let request = Request::execute(sql, params);
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        Ok(NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        })
    }

    /// Execute a SQL statement and return the last insert rowid (returns awaitable).
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<NativeExecuteAwaitable> {
        let params = extract_params(py, params)?;
        let request = Request::execute_returning_rowid(sql, params);
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        Ok(NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        })
    }

    /// Execute multiple SQL statements (returns awaitable).
    fn executescript(&self, sql: &str) -> NativeExecuteAwaitable {
        let request = Request::execute_batch(sql);
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Begin a transaction (returns awaitable).
    fn begin(&self) -> NativeExecuteAwaitable {
        let request = Request::begin_transaction();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Commit the current transaction (returns awaitable).
    fn commit(&self) -> NativeExecuteAwaitable {
        let request = Request::commit();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Rollback the current transaction (returns awaitable).
    fn rollback(&self) -> NativeExecuteAwaitable {
        let request = Request::rollback();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Ping the server (returns awaitable).
    fn ping(&self) -> NativeExecuteAwaitable {
        let request = Request::ping();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        NativeExecuteAwaitable {
            socket_path: self.socket_path.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }
}

/// Create a native async client (convenience function).
#[pyfunction]
fn native_async_client(socket_path: &str) -> PyNativeAsyncClient {
    PyNativeAsyncClient::new(socket_path)
}

// =============================================================================
// Persistent Native Awaitable Client - uses a single persistent connection
// =============================================================================

/// Shared socket connection with reader and writer.
struct SocketConnection {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
}

/// Native awaitable for execute operations using a persistent connection.
#[pyclass]
pub struct PersistentNativeExecuteAwaitable {
    connection: Arc<Mutex<Option<SocketConnection>>>,
    request_bytes: Vec<u8>,
    completed: bool,
    result: Option<(bool, i64, i64, Option<String>)>,
}

#[pymethods]
impl PersistentNativeExecuteAwaitable {
    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if slf.completed {
            if let Some(result) = slf.result.take() {
                let result_obj = result.to_object(py);
                return Err(make_stop_iteration(py, result_obj));
            }
            return Err(PyStopIteration::new_err(py.None()));
        }

        let connection = slf.connection.clone();
        let request_bytes = slf.request_bytes.clone();

        // Lock connection, release GIL, do I/O
        let result = py.allow_threads(move || {
            let mut conn_guard = connection.lock().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Lock poisoned")
            })?;

            let conn = conn_guard.as_mut().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "Connection closed")
            })?;

            use std::io::Write;
            conn.writer.write_all(&request_bytes)?;
            conn.writer.flush()?;

            let response_data = read_message(&mut conn.reader)?;
            let response = Response::deserialize(&response_data).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;

            Ok::<_, std::io::Error>((
                response.is_ok(),
                response.rows_affected,
                response.last_insert_rowid,
                response.error_message,
            ))
        });

        slf.completed = true;

        match result {
            Ok(result) => {
                slf.result = Some(result.clone());
                let result_obj = result.to_object(py);
                Err(make_stop_iteration(py, result_obj))
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Socket error: {}", e))),
        }
    }
}

/// Persistent native async client that reuses a single socket connection.
///
/// This is more efficient than NativeAsyncClient for multiple operations
/// because it doesn't create a new connection for each request.
#[pyclass(name = "PersistentNativeAsyncClient")]
pub struct PyPersistentNativeAsyncClient {
    connection: Arc<Mutex<Option<SocketConnection>>>,
}

#[pymethods]
impl PyPersistentNativeAsyncClient {
    #[new]
    fn new(socket_path: &str) -> PyResult<Self> {
        let stream = UnixStream::connect(socket_path).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to connect: {}", e))
        })?;
        let reader = BufReader::new(stream.try_clone().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to clone stream: {}", e))
        })?);
        let writer = BufWriter::new(stream);

        Ok(PyPersistentNativeAsyncClient {
            connection: Arc::new(Mutex::new(Some(SocketConnection { reader, writer }))),
        })
    }

    /// Execute a SQL statement asynchronously (returns awaitable).
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<PersistentNativeExecuteAwaitable> {
        let params = extract_params(py, params)?;
        let request = Request::execute(sql, params);
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        Ok(PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        })
    }

    /// Execute a SQL statement and return the last insert rowid (returns awaitable).
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<PersistentNativeExecuteAwaitable> {
        let params = extract_params(py, params)?;
        let request = Request::execute_returning_rowid(sql, params);
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        Ok(PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        })
    }

    /// Execute multiple SQL statements (returns awaitable).
    fn executescript(&self, sql: &str) -> PersistentNativeExecuteAwaitable {
        let request = Request::execute_batch(sql);
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Begin a transaction (returns awaitable).
    fn begin(&self) -> PersistentNativeExecuteAwaitable {
        let request = Request::begin_transaction();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Commit the current transaction (returns awaitable).
    fn commit(&self) -> PersistentNativeExecuteAwaitable {
        let request = Request::commit();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Rollback the current transaction (returns awaitable).
    fn rollback(&self) -> PersistentNativeExecuteAwaitable {
        let request = Request::rollback();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Ping the server (returns awaitable).
    fn ping(&self) -> PersistentNativeExecuteAwaitable {
        let request = Request::ping();
        let data = request.serialize();
        let mut request_bytes = (data.len() as u32).to_le_bytes().to_vec();
        request_bytes.extend(data);

        PersistentNativeExecuteAwaitable {
            connection: self.connection.clone(),
            request_bytes,
            completed: false,
            result: None,
        }
    }

    /// Close the connection.
    fn close(&self) -> PyResult<()> {
        let mut conn_guard = self.connection.lock().map_err(|_| {
            PyRuntimeError::new_err("Lock poisoned")
        })?;
        // Take the connection, dropping it will close the socket
        *conn_guard = None;
        Ok(())
    }
}

/// Create a persistent native async client (convenience function).
#[pyfunction]
fn persistent_native_async_client(socket_path: &str) -> PyResult<PyPersistentNativeAsyncClient> {
    PyPersistentNativeAsyncClient::new(socket_path)
}

/// Python module
#[pymodule]
fn pasyn_sqlite_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Connection classes
    m.add_class::<PyConnection>()?;
    m.add_class::<PyCursor>()?;
    m.add_class::<PyOpenFlags>()?;

    // Writer server and client classes
    m.add_class::<PyWriterServerHandle>()?;
    m.add_class::<PyWriterClient>()?;
    m.add_class::<PyHybridConnection>()?;

    // Native async client classes
    m.add_class::<PyNativeAsyncClient>()?;
    m.add_class::<NativeExecuteAwaitable>()?;
    m.add_class::<PyPersistentNativeAsyncClient>()?;
    m.add_class::<PersistentNativeExecuteAwaitable>()?;

    // Connection functions
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(hybrid_connect, m)?)?;

    // Server functions
    m.add_function(wrap_pyfunction!(start_writer_server, m)?)?;
    m.add_function(wrap_pyfunction!(default_socket_path, m)?)?;
    m.add_function(wrap_pyfunction!(native_async_client, m)?)?;
    m.add_function(wrap_pyfunction!(persistent_native_async_client, m)?)?;

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
