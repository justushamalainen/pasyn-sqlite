//! Python bindings for pasyn-sqlite-core using PyO3
//!
//! This module provides Python bindings to the SQLite functionality.

use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
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

    // Connection functions
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(hybrid_connect, m)?)?;

    // Server functions
    m.add_function(wrap_pyfunction!(start_writer_server, m)?)?;
    m.add_function(wrap_pyfunction!(default_socket_path, m)?)?;

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
