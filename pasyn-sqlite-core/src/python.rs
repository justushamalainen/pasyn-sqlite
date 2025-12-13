//! Python bindings for pasyn-sqlite-core using PyO3
//!
//! This module provides Python bindings to the SQLite functionality.

use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::cache;
use crate::client::MultiplexedClient as RustMultiplexedClient;
use crate::connection::Connection as RustConnection;
use crate::connection::OpenFlags as RustOpenFlags;
use crate::connection::ThreadSafeConnection;
use crate::error::Error as RustError;
use crate::ffi;
use crate::server::{ServerConfig, ServerHandle, WriterServer};
use crate::value::Value as RustValue;

use std::path::PathBuf;
use std::sync::Arc;

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

/// Convert all columns from raw statement pointer to Python tuple
/// Used with cached statements
#[inline]
fn raw_stmt_to_py_tuple(py: Python<'_>, stmt: *mut crate::ffi::sqlite3_stmt) -> Py<PyTuple> {
    use crate::ffi;

    let count = unsafe { ffi::sqlite3_column_count(stmt) as usize };
    let values: Vec<PyObject> = (0..count)
        .map(|i| {
            let col_type = unsafe { ffi::sqlite3_column_type(stmt, i as std::os::raw::c_int) };
            match col_type {
                ffi::SQLITE_NULL => py.None(),
                ffi::SQLITE_INTEGER => {
                    let v = unsafe { ffi::sqlite3_column_int64(stmt, i as std::os::raw::c_int) };
                    v.to_object(py)
                }
                ffi::SQLITE_FLOAT => {
                    let v = unsafe { ffi::sqlite3_column_double(stmt, i as std::os::raw::c_int) };
                    v.to_object(py)
                }
                ffi::SQLITE_TEXT => {
                    let ptr = unsafe { ffi::sqlite3_column_text(stmt, i as std::os::raw::c_int) };
                    if ptr.is_null() {
                        py.None()
                    } else {
                        let len =
                            unsafe { ffi::sqlite3_column_bytes(stmt, i as std::os::raw::c_int) };
                        let slice = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
                        match std::str::from_utf8(slice) {
                            Ok(s) => s.to_object(py),
                            Err(_) => py.None(),
                        }
                    }
                }
                ffi::SQLITE_BLOB => {
                    let ptr = unsafe { ffi::sqlite3_column_blob(stmt, i as std::os::raw::c_int) };
                    if ptr.is_null() {
                        PyBytes::new_bound(py, &[]).into_any().unbind()
                    } else {
                        let len =
                            unsafe { ffi::sqlite3_column_bytes(stmt, i as std::os::raw::c_int) };
                        let slice =
                            unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
                        PyBytes::new_bound(py, slice).into_any().unbind()
                    }
                }
                _ => py.None(),
            }
        })
        .collect();
    PyTuple::new_bound(py, &values).unbind()
}

/// Extract parameters from Python args
fn extract_params(py: Python<'_>, params: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<RustValue>> {
    match params {
        None => Ok(Vec::new()),
        Some(obj) => {
            if let Ok(list) = obj.downcast::<PyList>() {
                list.iter().map(|item| py_to_value(py, &item)).collect()
            } else if let Ok(tuple) = obj.downcast::<PyTuple>() {
                tuple.iter().map(|item| py_to_value(py, &item)).collect()
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

/// A SQLite database connection (thread-safe, no mutex needed)
///
/// This connection uses Arc<ThreadSafeConnection> which is both Send and Sync.
/// SQLite handles all internal locking via FULLMUTEX mode, and we use a
/// statement-per-query pattern to avoid borrowing issues.
#[pyclass(name = "Connection")]
pub struct PyConnection {
    conn: Arc<ThreadSafeConnection>,
}

#[pymethods]
impl PyConnection {
    /// Open a database connection
    #[new]
    #[pyo3(signature = (path, flags=None))]
    fn new(path: &str, flags: Option<PyOpenFlags>) -> PyResult<Self> {
        let flags = flags
            .map(|f| RustOpenFlags::from_bits(f.flags))
            .unwrap_or_default();
        let conn = ThreadSafeConnection::open_with_flags(path, flags).map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(conn),
        })
    }

    /// Open an in-memory database
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        let conn = ThreadSafeConnection::open_in_memory().map_err(to_py_err)?;
        Ok(PyConnection {
            conn: Arc::new(conn),
        })
    }

    /// Open a shared in-memory database
    #[staticmethod]
    fn shared_memory(name: &str) -> PyResult<Self> {
        // For shared memory, we need URI mode
        let uri = format!("file:{}?mode=memory&cache=shared", name);
        let flags = RustOpenFlags::default_readwrite().union(RustOpenFlags::URI);
        let conn = ThreadSafeConnection::open_with_flags(&uri, flags).map_err(to_py_err)?;
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
        self.conn.execute(sql, &params).map_err(to_py_err)
    }

    /// Execute multiple SQL statements
    fn executescript(&self, sql: &str) -> PyResult<()> {
        self.conn.execute_batch(sql).map_err(to_py_err)
    }

    /// Execute SQL and return all rows
    ///
    /// Uses cached prepared statements and direct SQLite→Python conversion
    /// for optimal performance on repeated queries.
    #[pyo3(signature = (sql, params=None))]
    fn execute_fetchall<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyList>> {
        let params = extract_params(py, params)?;

        // Use cached query for better performance on repeated queries
        let py_rows = self
            .conn
            .query_cached(sql, &params, |stmt| raw_stmt_to_py_tuple(py, stmt))
            .map_err(to_py_err)?;

        Ok(PyList::new_bound(py, &py_rows).unbind())
    }

    /// Execute SQL and return the first row
    ///
    /// Uses cached prepared statements for optimal performance.
    #[pyo3(signature = (sql, params=None))]
    fn execute_fetchone<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Option<Py<PyTuple>>> {
        let params = extract_params(py, params)?;

        // Use cached query for single row
        self.conn
            .query_cached_one(sql, &params, |stmt| raw_stmt_to_py_tuple(py, stmt))
            .map_err(to_py_err)
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

        // Get column names by doing a quick query (statement-per-query pattern)
        // We'll re-execute the query when actually fetching
        let column_names = self.get_column_names(sql)?;

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
    fn in_transaction(&self) -> PyResult<bool> {
        Ok(!self.conn.is_autocommit())
    }

    /// Get the last inserted row ID
    #[getter]
    fn last_insert_rowid(&self) -> PyResult<i64> {
        Ok(self.conn.last_insert_rowid())
    }

    /// Get the number of rows changed
    #[getter]
    fn changes(&self) -> PyResult<i64> {
        Ok(self.conn.changes())
    }

    /// Set busy timeout in milliseconds
    fn set_busy_timeout(&self, ms: i32) -> PyResult<()> {
        self.conn.busy_timeout(ms).map_err(to_py_err)
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

impl PyConnection {
    /// Helper to get column names for a query (used by cursor)
    fn get_column_names(&self, sql: &str) -> PyResult<Vec<String>> {
        // Use EXPLAIN to get column info without executing the query
        // This is a lightweight way to get schema info
        let explain_sql = format!("SELECT * FROM ({}) LIMIT 0", sql);
        match self.conn.query_fetchall(&explain_sql, &[]) {
            Ok(_) => {
                // For now, return empty - cursor will work without column names
                // A more complete implementation would parse EXPLAIN output
                Ok(Vec::new())
            }
            Err(_) => Ok(Vec::new()),
        }
    }
}

// =============================================================================
// Single-threaded Connection (Python-managed lifetime, no Arc)
// =============================================================================

/// A single-threaded SQLite connection with Python-managed lifetime
///
/// This connection is NOT thread-safe. It's owned directly by Python with no
/// Arc wrapper, giving minimal overhead. Use this when you don't need to share
/// the connection across threads.
#[pyclass(name = "SingleThreadConnection", unsendable)]
pub struct PySingleThreadConnection {
    db: *mut ffi::sqlite3,
}

// NOT Send/Sync - this is intentionally single-threaded
// Python's GIL ensures safety within a single thread

impl Drop for PySingleThreadConnection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            // Clear statement cache before closing
            cache::clear_cache_for_db(self.db);
            unsafe { ffi::sqlite3_close_v2(self.db) };
            self.db = std::ptr::null_mut();
        }
    }
}

#[pymethods]
impl PySingleThreadConnection {
    /// Open a database connection
    #[new]
    #[pyo3(signature = (path, flags=None))]
    fn new(path: &str, flags: Option<PyOpenFlags>) -> PyResult<Self> {
        let flags = flags
            .map(|f| f.flags)
            .unwrap_or(RustOpenFlags::default_readwrite().bits());

        let c_path = std::ffi::CString::new(path)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let mut db: *mut ffi::sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, std::ptr::null())
        };

        if rc != ffi::SQLITE_OK {
            if !db.is_null() {
                let err_msg = unsafe {
                    std::ffi::CStr::from_ptr(ffi::sqlite3_errmsg(db))
                        .to_string_lossy()
                        .to_string()
                };
                unsafe { ffi::sqlite3_close(db) };
                return Err(SqliteError::new_err(err_msg));
            }
            return Err(SqliteError::new_err(format!("SQLite error code: {}", rc)));
        }

        Ok(PySingleThreadConnection { db })
    }

    /// Open an in-memory database
    #[staticmethod]
    fn memory() -> PyResult<Self> {
        Self::new(":memory:", None)
    }

    /// Execute a SQL statement with optional parameters
    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<usize> {
        let params = extract_params(py, params)?;

        // Get cached statement
        let stmt = cache::get_cached_statement(self.db, sql)
            .map_err(to_py_err)?;

        // Bind parameters
        self.bind_params(stmt, &params)?;

        // Execute
        let rc = unsafe { ffi::sqlite3_step(stmt) };
        if rc != ffi::SQLITE_DONE && rc != ffi::SQLITE_ROW {
            return Err(self.get_error());
        }

        Ok(unsafe { ffi::sqlite3_changes(self.db) } as usize)
    }

    /// Execute multiple SQL statements (no parameters)
    fn executescript(&self, sql: &str) -> PyResult<()> {
        let c_sql = std::ffi::CString::new(sql)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let rc = unsafe {
            ffi::sqlite3_exec(
                self.db,
                c_sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        if rc != ffi::SQLITE_OK {
            return Err(self.get_error());
        }
        Ok(())
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

        // Get cached statement
        let stmt = cache::get_cached_statement(self.db, sql)
            .map_err(to_py_err)?;

        // Bind parameters
        self.bind_params(stmt, &params)?;

        // Collect rows
        let mut rows: Vec<Py<PyTuple>> = Vec::new();
        loop {
            let rc = unsafe { ffi::sqlite3_step(stmt) };
            match rc {
                ffi::SQLITE_ROW => {
                    rows.push(raw_stmt_to_py_tuple(py, stmt));
                }
                ffi::SQLITE_DONE => break,
                _ => return Err(self.get_error()),
            }
        }

        Ok(PyList::new_bound(py, &rows).unbind())
    }

    /// Execute SQL and return first row
    #[pyo3(signature = (sql, params=None))]
    fn execute_fetchone<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Option<Py<PyTuple>>> {
        let params = extract_params(py, params)?;

        // Get cached statement
        let stmt = cache::get_cached_statement(self.db, sql)
            .map_err(to_py_err)?;

        // Bind parameters
        self.bind_params(stmt, &params)?;

        // Get first row
        let rc = unsafe { ffi::sqlite3_step(stmt) };
        match rc {
            ffi::SQLITE_ROW => Ok(Some(raw_stmt_to_py_tuple(py, stmt))),
            ffi::SQLITE_DONE => Ok(None),
            _ => Err(self.get_error()),
        }
    }

    /// Begin a transaction
    fn begin(&self) -> PyResult<()> {
        self.executescript("BEGIN")
    }

    /// Commit the current transaction
    fn commit(&self) -> PyResult<()> {
        self.executescript("COMMIT")
    }

    /// Rollback the current transaction
    fn rollback(&self) -> PyResult<()> {
        self.executescript("ROLLBACK")
    }

    /// Check if in autocommit mode
    #[getter]
    fn in_transaction(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.db) == 0 }
    }

    /// Get the last inserted row ID
    #[getter]
    fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db) }
    }

    /// Get the number of rows changed
    #[getter]
    fn changes(&self) -> i64 {
        unsafe { ffi::sqlite3_changes(self.db) as i64 }
    }

    /// Close the connection
    fn close(&mut self) -> PyResult<()> {
        if !self.db.is_null() {
            cache::clear_cache_for_db(self.db);
            let rc = unsafe { ffi::sqlite3_close(self.db) };
            if rc != ffi::SQLITE_OK {
                return Err(SqliteError::new_err("Failed to close database"));
            }
            self.db = std::ptr::null_mut();
        }
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        Ok(false)
    }
}

impl PySingleThreadConnection {
    /// Bind parameters to a statement
    fn bind_params(&self, stmt: *mut ffi::sqlite3_stmt, params: &[RustValue]) -> PyResult<()> {
        for (i, param) in params.iter().enumerate() {
            let idx = (i + 1) as i32;
            let rc = match param {
                RustValue::Null => unsafe { ffi::sqlite3_bind_null(stmt, idx) },
                RustValue::Integer(v) => unsafe { ffi::sqlite3_bind_int64(stmt, idx, *v) },
                RustValue::Real(v) => unsafe { ffi::sqlite3_bind_double(stmt, idx, *v) },
                RustValue::Text(v) => {
                    let c_str = std::ffi::CString::new(v.as_str())
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    unsafe {
                        ffi::sqlite3_bind_text(
                            stmt,
                            idx,
                            c_str.as_ptr(),
                            v.len() as i32,
                            ffi::sqlite_transient(),
                        )
                    }
                }
                RustValue::Blob(v) => unsafe {
                    ffi::sqlite3_bind_blob(
                        stmt,
                        idx,
                        v.as_ptr() as *const std::ffi::c_void,
                        v.len() as i32,
                        ffi::sqlite_transient(),
                    )
                },
            };
            if rc != ffi::SQLITE_OK {
                return Err(self.get_error());
            }
        }
        Ok(())
    }

    /// Get current error message
    fn get_error(&self) -> PyErr {
        let msg = unsafe {
            std::ffi::CStr::from_ptr(ffi::sqlite3_errmsg(self.db))
                .to_string_lossy()
                .to_string()
        };
        SqliteError::new_err(msg)
    }
}

/// A database cursor for iterating over query results
#[pyclass(name = "Cursor")]
pub struct PyCursor {
    conn: Arc<ThreadSafeConnection>,
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
    ///
    /// Uses cached prepared statements for optimal performance.
    fn fetchall<'py>(&mut self, py: Python<'py>) -> PyResult<Py<PyList>> {
        if !self.executed {
            self.execute()?;
        }

        // Use cached query
        let py_rows = self
            .conn
            .query_cached(&self.sql, &self.params, |stmt| raw_stmt_to_py_tuple(py, stmt))
            .map_err(to_py_err)?;

        Ok(PyList::new_bound(py, &py_rows).unbind())
    }

    /// Fetch the next row
    ///
    /// Uses cached prepared statements for optimal performance.
    fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Py<PyTuple>>> {
        if !self.executed {
            self.execute()?;
        }

        // Use cached query for single row
        self.conn
            .query_cached_one(&self.sql, &self.params, |stmt| raw_stmt_to_py_tuple(py, stmt))
            .map_err(to_py_err)
    }

    /// Fetch many rows
    ///
    /// Uses cached prepared statements for optimal performance.
    #[pyo3(signature = (size=None))]
    fn fetchmany<'py>(&mut self, py: Python<'py>, size: Option<usize>) -> PyResult<Py<PyList>> {
        let size = size.unwrap_or(100);

        if !self.executed {
            self.execute()?;
        }

        // For fetchmany, we need to limit the results
        // Add LIMIT to the query if not already present
        let limited_sql = if self.sql.to_uppercase().contains("LIMIT") {
            self.sql.clone()
        } else {
            format!("{} LIMIT {}", self.sql, size)
        };

        // Use cached query
        let py_rows = self
            .conn
            .query_cached(&limited_sql, &self.params, |stmt| raw_stmt_to_py_tuple(py, stmt))
            .map_err(to_py_err)?;

        // Take only up to 'size' rows
        let limited: Vec<_> = py_rows.into_iter().take(size).collect();
        Ok(PyList::new_bound(py, &limited).unbind())
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
                )
                .unbind())
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
            handle
                .join()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to join server: {}", e)))?;
        }
        Ok(())
    }

    /// Shutdown and wait for the server to stop
    fn stop(&mut self) -> PyResult<()> {
        if let Some(handle) = self.handle.take() {
            handle
                .stop()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to stop server: {}", e)))?;
        }
        Ok(())
    }
}

/// Start a writer server that handles all write operations via Unix socket
#[pyfunction]
#[pyo3(signature = (database_path, socket_path=None))]
fn start_writer_server(
    database_path: &str,
    socket_path: Option<&str>,
) -> PyResult<PyWriterServerHandle> {
    let socket_path = socket_path
        .map(PathBuf::from)
        .unwrap_or_else(|| ServerConfig::default_socket_path(database_path));

    let server = WriterServer::new(database_path, &socket_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create server: {}", e)))?;

    let handle = server
        .spawn()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to start server: {}", e)))?;

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
    ServerConfig::default_socket_path(database_path)
        .to_string_lossy()
        .to_string()
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
    let response = Response::deserialize(data)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to parse response: {}", e)))?;
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
        let client = RustMultiplexedClient::connect(socket_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))?;
        Ok(PyMultiplexedClient {
            client: Arc::new(client),
        })
    }

    /// Execute a SQL statement. Returns the number of rows affected.
    ///
    /// This method is thread-safe and can be called from multiple threads concurrently.
    /// The GIL is released during I/O operations.
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let params = extract_params(py, params)?;
        let client = self.client.clone();

        // Release GIL during blocking I/O
        py.allow_threads(move || {
            client
                .execute(&sql, params)
                .map(|rows| rows as i64)
                .map_err(to_py_err)
        })
    }

    /// Execute a SQL statement and return the last insert rowid.
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let params = extract_params(py, params)?;
        let client = self.client.clone();

        py.allow_threads(move || {
            client
                .execute_returning_rowid(&sql, params)
                .map_err(to_py_err)
        })
    }

    /// Execute the same SQL statement with multiple parameter sets.
    ///
    /// This is more efficient than calling execute() multiple times because
    /// the statement is prepared once and reused for each parameter set.
    /// Returns the total number of rows affected.
    fn execute_many(
        &self,
        py: Python<'_>,
        sql: String,
        params_batch: &Bound<'_, PyList>,
    ) -> PyResult<i64> {
        // Convert Python list of parameter lists to Vec<Vec<Value>>
        let batch: Vec<Vec<RustValue>> = params_batch
            .iter()
            .map(|item| extract_params(py, Some(&item)))
            .collect::<PyResult<_>>()?;

        let client = self.client.clone();

        py.allow_threads(move || {
            client
                .execute_many(&sql, batch)
                .map(|rows| rows as i64)
                .map_err(to_py_err)
        })
    }

    /// Execute multiple SQL statements (batch/script).
    fn executescript(&self, py: Python<'_>, sql: String) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || client.execute_batch(&sql).map_err(to_py_err))
    }

    /// Begin a transaction.
    fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || client.begin_transaction().map_err(to_py_err))
    }

    /// Commit the current transaction.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || client.commit().map_err(to_py_err))
    }

    /// Rollback the current transaction.
    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || client.rollback().map_err(to_py_err))
    }

    /// Ping the server to check if it's alive.
    fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || client.ping().map_err(to_py_err))
    }

    /// Request the server to shutdown.
    fn shutdown_server(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();

        py.allow_threads(move || client.shutdown().map_err(to_py_err))
    }

    /// Acquire an exclusive transaction lock.
    ///
    /// Returns a Transaction object that holds the lock. While the lock is held,
    /// no other clients can perform write operations. The transaction must be
    /// committed or rolled back within the server's timeout period (default: 1 second).
    ///
    /// Example:
    /// ```python
    /// tx = client.begin_exclusive()
    /// try:
    ///     tx.execute("INSERT INTO users VALUES (?, ?)", [1, "Alice"])
    ///     tx.execute("INSERT INTO users VALUES (?, ?)", [2, "Bob"])
    ///     tx.commit()
    /// except Exception:
    ///     tx.rollback()
    ///     raise
    /// ```
    fn begin_exclusive(&self, py: Python<'_>) -> PyResult<PyTransaction> {
        let client = self.client.clone();

        py.allow_threads(move || {
            let tx = client.begin_exclusive().map_err(to_py_err)?;
            let token = tx.token();
            // We need to prevent the Transaction from rolling back when dropped
            // by converting it to just the token
            std::mem::forget(tx);
            Ok(PyTransaction {
                client,
                token,
                finished: false,
            })
        })
    }
}

/// An exclusive transaction guard.
///
/// This class represents an exclusive transaction lock. While this transaction exists,
/// no other clients can perform write operations on the database.
///
/// The transaction must be explicitly committed with `commit()`. If the object
/// is dropped (e.g., garbage collected) without committing, the transaction will
/// be automatically rolled back.
///
/// Can be used as a context manager:
/// ```python
/// with client.begin_exclusive() as tx:
///     tx.execute("INSERT INTO users VALUES (?, ?)", [1, "Alice"])
///     tx.commit()
/// ```
#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    client: Arc<RustMultiplexedClient>,
    token: u64,
    finished: bool,
}

#[pymethods]
impl PyTransaction {
    /// Get the transaction token (for debugging/logging).
    #[getter]
    fn token(&self) -> u64 {
        self.token
    }

    /// Check if the transaction has been finished (committed or rolled back).
    #[getter]
    fn finished(&self) -> bool {
        self.finished
    }

    /// Execute a SQL statement within this transaction.
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        if self.finished {
            return Err(PyRuntimeError::new_err("Transaction already finished"));
        }
        let params = extract_params(py, params)?;
        let client = self.client.clone();
        let token = self.token;

        py.allow_threads(move || {
            client
                .execute_with_token(&sql, params, token)
                .map(|rows| rows as i64)
                .map_err(to_py_err)
        })
    }

    /// Execute a SQL statement and return the last insert rowid.
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rowid(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        if self.finished {
            return Err(PyRuntimeError::new_err("Transaction already finished"));
        }
        let params = extract_params(py, params)?;
        let client = self.client.clone();
        let token = self.token;

        py.allow_threads(move || {
            client
                .execute_returning_rowid_with_token(&sql, params, token)
                .map_err(to_py_err)
        })
    }

    /// Execute multiple SQL statements (batch/script) within this transaction.
    fn executescript(&self, py: Python<'_>, sql: String) -> PyResult<()> {
        if self.finished {
            return Err(PyRuntimeError::new_err("Transaction already finished"));
        }
        let client = self.client.clone();
        let token = self.token;

        py.allow_threads(move || {
            client
                .execute_batch_with_token(&sql, token)
                .map_err(to_py_err)
        })
    }

    /// Commit the transaction and release the lock.
    fn commit(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.finished {
            return Err(PyRuntimeError::new_err("Transaction already finished"));
        }
        let client = self.client.clone();
        let token = self.token;

        py.allow_threads(move || client.commit_with_token(token).map_err(to_py_err))?;
        self.finished = true;
        Ok(())
    }

    /// Rollback the transaction and release the lock.
    fn rollback(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.finished {
            return Err(PyRuntimeError::new_err("Transaction already finished"));
        }
        let client = self.client.clone();
        let token = self.token;

        py.allow_threads(move || client.rollback_with_token(token).map_err(to_py_err))?;
        self.finished = true;
        Ok(())
    }

    /// Context manager support: enter
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager support: exit (auto-rollback if not committed)
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        // If already finished, nothing to do
        if self.finished {
            return Ok(false);
        }

        // If there was an exception, rollback
        if exc_type.is_some() {
            let _ = self.rollback(py);
        }
        // If no exception but not committed, also rollback
        else if !self.finished {
            let _ = self.rollback(py);
        }

        Ok(false) // Don't suppress exceptions
    }
}

impl Drop for PyTransaction {
    fn drop(&mut self) {
        if !self.finished {
            // Auto-rollback on drop (best effort, ignore errors)
            let _ = self.client.rollback_with_token(self.token);
        }
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
    m.add_class::<PySingleThreadConnection>()?;
    m.add_class::<PyCursor>()?;
    m.add_class::<PyOpenFlags>()?;

    // Writer server class
    m.add_class::<PyWriterServerHandle>()?;

    // Multiplexed client class
    m.add_class::<PyMultiplexedClient>()?;

    // Transaction class
    m.add_class::<PyTransaction>()?;

    // Connection functions
    m.add_function(wrap_pyfunction!(connect, m)?)?;

    // Server functions
    m.add_function(wrap_pyfunction!(start_writer_server, m)?)?;
    m.add_function(wrap_pyfunction!(default_socket_path, m)?)?;
    m.add_function(wrap_pyfunction!(multiplexed_client, m)?)?;

    // Protocol serialization functions (for async I/O)
    m.add_function(wrap_pyfunction!(serialize_execute_request, m)?)?;
    m.add_function(wrap_pyfunction!(
        serialize_execute_returning_rowid_request,
        m
    )?)?;
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
