//! Safe wrapper for SQLite prepared statements
//!
//! This module provides a safe interface to SQLite prepared statements.

use std::ffi::{CStr, CString};
use std::os::raw::c_int;
use std::ptr;

use crate::error::{Error, Result};
use crate::ffi;
use crate::value::Value;

/// A prepared SQL statement
///
/// Prepared statements are compiled SQL that can be executed multiple times
/// with different parameters.
pub struct Statement {
    stmt: *mut ffi::sqlite3_stmt,
    db: *mut ffi::sqlite3,
}

// Safety: SQLite statements are thread-safe when the database is opened with
// appropriate mutex settings (which we do by default)
unsafe impl Send for Statement {}

impl Statement {
    /// Prepare a new statement from SQL
    pub(crate) fn prepare(db: *mut ffi::sqlite3, sql: &str) -> Result<Self> {
        let c_sql = CString::new(sql)?;
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();

        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                db,
                c_sql.as_ptr(),
                c_sql.as_bytes().len() as c_int,
                &mut stmt,
                ptr::null_mut(),
            )
        };

        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(db) });
        }

        Ok(Statement { stmt, db })
    }

    /// Execute the statement without returning rows
    ///
    /// Returns the number of rows changed.
    pub fn execute<P: IntoIterator>(&mut self, params: P) -> Result<usize>
    where
        P::Item: Into<Value>,
    {
        self.reset()?;
        self.bind_all(params)?;

        loop {
            let rc = unsafe { ffi::sqlite3_step(self.stmt) };
            match rc {
                ffi::SQLITE_DONE => break,
                ffi::SQLITE_ROW => continue, // Skip any returned rows
                _ => return Err(unsafe { Error::from_db(self.db) }),
            }
        }

        Ok(unsafe { ffi::sqlite3_changes(self.db) as usize })
    }

    /// Step to the next row
    ///
    /// Returns true if there is a row available, false if done.
    pub fn step(&mut self) -> Result<bool> {
        let rc = unsafe { ffi::sqlite3_step(self.stmt) };
        match rc {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            _ => Err(unsafe { Error::from_db(self.db) }),
        }
    }

    /// Reset the statement for re-execution
    pub fn reset(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_reset(self.stmt) };
        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(self.db) });
        }
        Ok(())
    }

    /// Clear all bindings
    pub fn clear_bindings(&mut self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_clear_bindings(self.stmt) };
        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(self.db) });
        }
        Ok(())
    }

    /// Bind all parameters from an iterator
    pub fn bind_all<P: IntoIterator>(&mut self, params: P) -> Result<()>
    where
        P::Item: Into<Value>,
    {
        for (i, param) in params.into_iter().enumerate() {
            self.bind(i + 1, param.into())?;
        }
        Ok(())
    }

    /// Bind a single parameter by index (1-based)
    pub fn bind(&mut self, index: usize, value: Value) -> Result<()> {
        let idx = index as c_int;
        let rc = match value {
            Value::Null => unsafe { ffi::sqlite3_bind_null(self.stmt, idx) },
            Value::Integer(i) => unsafe { ffi::sqlite3_bind_int64(self.stmt, idx, i) },
            Value::Real(f) => unsafe { ffi::sqlite3_bind_double(self.stmt, idx, f) },
            Value::Text(ref s) => {
                let bytes = s.as_bytes();
                unsafe {
                    ffi::sqlite3_bind_text(
                        self.stmt,
                        idx,
                        bytes.as_ptr() as *const _,
                        bytes.len() as c_int,
                        ffi::sqlite_transient(),
                    )
                }
            }
            Value::Blob(ref b) => unsafe {
                ffi::sqlite3_bind_blob(
                    self.stmt,
                    idx,
                    b.as_ptr() as *const _,
                    b.len() as c_int,
                    ffi::sqlite_transient(),
                )
            },
        };

        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(self.db) });
        }
        Ok(())
    }

    /// Bind a parameter by name
    pub fn bind_named(&mut self, name: &str, value: Value) -> Result<()> {
        let c_name = CString::new(name)?;
        let idx = unsafe { ffi::sqlite3_bind_parameter_index(self.stmt, c_name.as_ptr()) };
        if idx == 0 {
            return Err(Error::with_message(
                crate::error::ErrorCode::Range,
                format!("Unknown parameter: {}", name),
            ));
        }
        self.bind(idx as usize, value)
    }

    /// Get the number of columns in the result
    pub fn column_count(&self) -> usize {
        unsafe { ffi::sqlite3_column_count(self.stmt) as usize }
    }

    /// Get a column name by index
    pub fn column_name(&self, index: usize) -> Option<&str> {
        let ptr = unsafe { ffi::sqlite3_column_name(self.stmt, index as c_int) };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ptr).to_str().ok()? })
        }
    }

    /// Get the type of a column
    pub fn column_type(&self, index: usize) -> ColumnType {
        let type_code = unsafe { ffi::sqlite3_column_type(self.stmt, index as c_int) };
        match type_code {
            ffi::SQLITE_INTEGER => ColumnType::Integer,
            ffi::SQLITE_FLOAT => ColumnType::Float,
            ffi::SQLITE_TEXT => ColumnType::Text,
            ffi::SQLITE_BLOB => ColumnType::Blob,
            ffi::SQLITE_NULL => ColumnType::Null,
            _ => ColumnType::Null,
        }
    }

    /// Check if a column is null
    pub fn column_is_null(&self, index: usize) -> bool {
        self.column_type(index) == ColumnType::Null
    }

    /// Get an integer column value
    pub fn column_int(&self, index: usize) -> i32 {
        unsafe { ffi::sqlite3_column_int(self.stmt, index as c_int) }
    }

    /// Get a 64-bit integer column value
    pub fn column_int64(&self, index: usize) -> i64 {
        unsafe { ffi::sqlite3_column_int64(self.stmt, index as c_int) }
    }

    /// Get a double column value
    pub fn column_double(&self, index: usize) -> f64 {
        unsafe { ffi::sqlite3_column_double(self.stmt, index as c_int) }
    }

    /// Get a text column value
    pub fn column_text(&self, index: usize) -> Result<&str> {
        let ptr = unsafe { ffi::sqlite3_column_text(self.stmt, index as c_int) };
        if ptr.is_null() {
            Ok("")
        } else {
            let len = unsafe { ffi::sqlite3_column_bytes(self.stmt, index as c_int) };
            let slice = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
            std::str::from_utf8(slice).map_err(|e| e.into())
        }
    }

    /// Get a blob column value
    pub fn column_blob(&self, index: usize) -> &[u8] {
        let ptr = unsafe { ffi::sqlite3_column_blob(self.stmt, index as c_int) };
        if ptr.is_null() {
            &[]
        } else {
            let len = unsafe { ffi::sqlite3_column_bytes(self.stmt, index as c_int) };
            unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
        }
    }

    /// Get a column value as a Value enum
    pub fn column_value(&self, index: usize) -> Value {
        match self.column_type(index) {
            ColumnType::Null => Value::Null,
            ColumnType::Integer => Value::Integer(self.column_int64(index)),
            ColumnType::Float => Value::Real(self.column_double(index)),
            ColumnType::Text => Value::Text(self.column_text(index).unwrap_or("").to_string()),
            ColumnType::Blob => Value::Blob(self.column_blob(index).to_vec()),
        }
    }

    /// Get all column values as a vector
    pub fn columns(&self) -> Vec<Value> {
        (0..self.column_count())
            .map(|i| self.column_value(i))
            .collect()
    }

    /// Get the number of parameters in the statement
    pub fn parameter_count(&self) -> usize {
        unsafe { ffi::sqlite3_bind_parameter_count(self.stmt) as usize }
    }

    /// Get the SQL text of the statement
    pub fn sql(&self) -> Option<&str> {
        let ptr = unsafe { ffi::sqlite3_sql(self.stmt) };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ptr).to_str().ok()? })
        }
    }

    /// Check if the statement is read-only
    pub fn is_readonly(&self) -> bool {
        unsafe { ffi::sqlite3_stmt_readonly(self.stmt) != 0 }
    }

    /// Check if the statement is currently executing
    pub fn is_busy(&self) -> bool {
        unsafe { ffi::sqlite3_stmt_busy(self.stmt) != 0 }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            unsafe { ffi::sqlite3_finalize(self.stmt) };
            self.stmt = ptr::null_mut();
        }
    }
}

/// SQLite column types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// NULL value
    Null,
    /// 64-bit signed integer
    Integer,
    /// 64-bit IEEE floating point
    Float,
    /// UTF-8 text string
    Text,
    /// Binary blob
    Blob,
}

/// Iterator over statement rows
pub struct Rows<'stmt> {
    stmt: &'stmt mut Statement,
    done: bool,
}

impl<'stmt> Rows<'stmt> {
    /// Create a new row iterator
    pub fn new(stmt: &'stmt mut Statement) -> Self {
        Rows { stmt, done: false }
    }

    /// Get the next row
    pub fn next_row(&mut self) -> Result<Option<RowRef<'_>>> {
        if self.done {
            return Ok(None);
        }

        match self.stmt.step()? {
            true => Ok(Some(RowRef { stmt: self.stmt })),
            false => {
                self.done = true;
                Ok(None)
            }
        }
    }
}

/// A reference to a row in the result set
pub struct RowRef<'stmt> {
    stmt: &'stmt Statement,
}

impl<'stmt> RowRef<'stmt> {
    /// Get a column value by index
    pub fn get<T: crate::connection::FromColumn>(&self, index: usize) -> Result<T> {
        T::from_column(self.stmt, index)
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }

    /// Get a column name
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.stmt.column_name(index)
    }

    /// Get the column type
    pub fn column_type(&self, index: usize) -> ColumnType {
        self.stmt.column_type(index)
    }

    /// Get all column values
    pub fn columns(&self) -> Vec<Value> {
        self.stmt.columns()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;

    #[test]
    fn test_prepare() {
        let conn = Connection::open_in_memory().unwrap();
        let stmt = conn.prepare("SELECT 1 + 1").unwrap();
        assert_eq!(stmt.column_count(), 1);
        assert!(!stmt.is_busy());
        assert!(stmt.is_readonly());
    }

    #[test]
    fn test_step() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT 1 + 1").unwrap();

        assert!(stmt.step().unwrap());
        assert_eq!(stmt.column_int(0), 2);
        assert!(!stmt.step().unwrap());
    }

    #[test]
    fn test_bind() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER, name TEXT, data BLOB)")
            .unwrap();

        let mut stmt = conn.prepare("INSERT INTO test VALUES (?1, ?2, ?3)").unwrap();
        stmt.bind(1, Value::Integer(42)).unwrap();
        stmt.bind(2, Value::Text("hello".to_string())).unwrap();
        stmt.bind(3, Value::Blob(vec![1, 2, 3])).unwrap();
        stmt.step().unwrap();

        let mut query = conn.prepare("SELECT * FROM test").unwrap();
        assert!(query.step().unwrap());
        assert_eq!(query.column_int64(0), 42);
        assert_eq!(query.column_text(1).unwrap(), "hello");
        assert_eq!(query.column_blob(2), &[1, 2, 3]);
    }

    #[test]
    fn test_column_types() {
        let conn = Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT 1, 1.5, 'text', X'0102', NULL")
            .unwrap();

        assert!(stmt.step().unwrap());
        assert_eq!(stmt.column_type(0), ColumnType::Integer);
        assert_eq!(stmt.column_type(1), ColumnType::Float);
        assert_eq!(stmt.column_type(2), ColumnType::Text);
        assert_eq!(stmt.column_type(3), ColumnType::Blob);
        assert_eq!(stmt.column_type(4), ColumnType::Null);
    }
}
