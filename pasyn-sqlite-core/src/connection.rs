//! Safe wrapper for SQLite database connections
//!
//! This module provides a safe, ergonomic Rust interface to SQLite connections.

use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;

use crate::error::{Error, ErrorCode, Result};
use crate::ffi;
use crate::statement::Statement;
use crate::value::Value;

/// Flags for opening a database connection
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags(i32);

impl OpenFlags {
    /// Open for reading only
    pub const READONLY: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_READONLY);
    /// Open for reading and writing
    pub const READWRITE: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_READWRITE);
    /// Create the database if it doesn't exist
    pub const CREATE: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_CREATE);
    /// Open with URI filename interpretation
    pub const URI: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_URI);
    /// Open in memory
    pub const MEMORY: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_MEMORY);
    /// Disable mutex
    pub const NOMUTEX: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_NOMUTEX);
    /// Full mutex
    pub const FULLMUTEX: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_FULLMUTEX);
    /// Shared cache
    pub const SHAREDCACHE: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_SHAREDCACHE);
    /// Private cache
    pub const PRIVATECACHE: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_PRIVATECACHE);
    /// Do not follow symlinks
    pub const NOFOLLOW: OpenFlags = OpenFlags(ffi::SQLITE_OPEN_NOFOLLOW);

    /// Default flags for read-write access (READWRITE | CREATE)
    pub const fn default_readwrite() -> Self {
        OpenFlags(ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE)
    }

    /// Combine flags
    pub const fn union(self, other: OpenFlags) -> OpenFlags {
        OpenFlags(self.0 | other.0)
    }

    /// Get raw flags value
    pub const fn bits(self) -> i32 {
        self.0
    }

    /// Create flags from a raw value
    pub const fn from_bits(bits: i32) -> Self {
        OpenFlags(bits)
    }
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self::default_readwrite()
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        OpenFlags(self.0 | rhs.0)
    }
}

/// A SQLite database connection
///
/// This is the main entry point for interacting with a SQLite database.
/// The connection is automatically closed when dropped.
pub struct Connection {
    db: *mut ffi::sqlite3,
}

// Safety: SQLite is thread-safe when compiled with SQLITE_THREADSAFE=1
// (which is the default and what we require)
unsafe impl Send for Connection {}

impl Connection {
    /// Open a database connection to a file path
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use pasyn_sqlite_core::Connection;
    ///
    /// let conn = Connection::open("my_database.db")?;
    /// # Ok::<(), pasyn_sqlite_core::Error>(())
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_flags(path, OpenFlags::default())
    }

    /// Open a database connection with specific flags
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy();
        let c_path = CString::new(path_str.as_bytes())?;

        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        let rc =
            unsafe { ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags.bits(), ptr::null()) };

        if rc != ffi::SQLITE_OK {
            // Even on error, we might get a db handle that needs to be closed
            if !db.is_null() {
                let err = unsafe { Error::from_db(db) };
                unsafe { ffi::sqlite3_close(db) };
                return Err(err);
            }
            return Err(Error::new(rc));
        }

        Ok(Connection { db })
    }

    /// Open an in-memory database
    ///
    /// # Examples
    ///
    /// ```
    /// use pasyn_sqlite_core::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// # Ok::<(), pasyn_sqlite_core::Error>(())
    /// ```
    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Open a shared in-memory database with a name
    ///
    /// Multiple connections can share the same in-memory database using the same name.
    pub fn open_shared_memory(name: &str) -> Result<Self> {
        let uri = format!("file:{}?mode=memory&cache=shared", name);
        Self::open_with_flags(&uri, OpenFlags::default_readwrite().union(OpenFlags::URI))
    }

    /// Execute a SQL statement that doesn't return any data
    ///
    /// # Examples
    ///
    /// ```
    /// use pasyn_sqlite_core::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    /// conn.execute("INSERT INTO users (name) VALUES (?1)", ["Alice"])?;
    /// # Ok::<(), pasyn_sqlite_core::Error>(())
    /// ```
    pub fn execute<P: IntoIterator>(&self, sql: &str, params: P) -> Result<usize>
    where
        P::Item: Into<Value>,
    {
        let mut stmt = self.prepare(sql)?;
        stmt.execute(params)
    }

    /// Execute the same SQL statement with multiple parameter sets
    ///
    /// This is more efficient than calling execute() multiple times because
    /// the statement is prepared once and reused for each parameter set.
    ///
    /// # Examples
    ///
    /// ```
    /// use pasyn_sqlite_core::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    /// conn.execute_many(
    ///     "INSERT INTO users (name) VALUES (?1)",
    ///     vec![vec!["Alice".into()], vec!["Bob".into()], vec!["Carol".into()]]
    /// )?;
    /// # Ok::<(), pasyn_sqlite_core::Error>(())
    /// ```
    pub fn execute_many(&self, sql: &str, params_batch: Vec<Vec<Value>>) -> Result<usize> {
        let mut stmt = self.prepare(sql)?;
        let mut total_changes = 0usize;

        for params in params_batch {
            total_changes += stmt.execute(params)?;
        }

        Ok(total_changes)
    }

    /// Execute a SQL script (multiple statements)
    ///
    /// This is useful for running DDL scripts or multiple statements at once.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let c_sql = CString::new(sql)?;
        let mut errmsg: *mut std::os::raw::c_char = ptr::null_mut();

        let rc = unsafe {
            ffi::sqlite3_exec(self.db, c_sql.as_ptr(), None, ptr::null_mut(), &mut errmsg)
        };

        if rc != ffi::SQLITE_OK {
            let message = if !errmsg.is_null() {
                let msg = unsafe { CStr::from_ptr(errmsg) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { ffi::sqlite3_free(errmsg as *mut _) };
                msg
            } else {
                "Unknown error".to_string()
            };

            return Err(Error::with_message(ErrorCode::from_raw(rc), message));
        }

        Ok(())
    }

    /// Prepare a SQL statement for execution
    ///
    /// # Examples
    ///
    /// ```
    /// use pasyn_sqlite_core::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// let mut stmt = conn.prepare("SELECT 1 + 1")?;
    /// # Ok::<(), pasyn_sqlite_core::Error>(())
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        Statement::prepare(self.db, sql)
    }

    /// Query the database and return results
    ///
    /// # Examples
    ///
    /// ```
    /// use pasyn_sqlite_core::Connection;
    ///
    /// let conn = Connection::open_in_memory()?;
    /// conn.execute_batch("CREATE TABLE users (id INTEGER, name TEXT)")?;
    /// conn.execute_batch("INSERT INTO users VALUES (1, 'Alice')")?;
    ///
    /// let mut stmt = conn.prepare("SELECT * FROM users WHERE id = ?1")?;
    /// // Use stmt to iterate over results
    /// # Ok::<(), pasyn_sqlite_core::Error>(())
    /// ```
    pub fn query<P: IntoIterator>(&self, sql: &str, params: P) -> Result<Statement>
    where
        P::Item: Into<Value>,
    {
        let mut stmt = self.prepare(sql)?;
        stmt.bind_all(params)?;
        Ok(stmt)
    }

    /// Query a single row and return Option<Row>
    pub fn query_row<P, F, T>(&self, sql: &str, params: P, f: F) -> Result<Option<T>>
    where
        P: IntoIterator,
        P::Item: Into<Value>,
        F: FnOnce(&Row) -> Result<T>,
    {
        let mut stmt = self.query(sql, params)?;
        if stmt.step()? {
            let row = Row::new(&stmt);
            Ok(Some(f(&row)?))
        } else {
            Ok(None)
        }
    }

    /// Begin a transaction
    pub fn begin_transaction(&self) -> Result<Transaction> {
        self.execute_batch("BEGIN")?;
        Ok(Transaction {
            conn: self,
            committed: false,
        })
    }

    /// Begin an immediate transaction
    pub fn begin_immediate(&self) -> Result<Transaction> {
        self.execute_batch("BEGIN IMMEDIATE")?;
        Ok(Transaction {
            conn: self,
            committed: false,
        })
    }

    /// Begin an exclusive transaction
    pub fn begin_exclusive(&self) -> Result<Transaction> {
        self.execute_batch("BEGIN EXCLUSIVE")?;
        Ok(Transaction {
            conn: self,
            committed: false,
        })
    }

    /// Check if the connection is in autocommit mode
    pub fn is_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.db) != 0 }
    }

    /// Get the rowid of the last inserted row
    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db) }
    }

    /// Get the number of rows changed by the last statement
    pub fn changes(&self) -> i64 {
        unsafe { ffi::sqlite3_changes64(self.db) }
    }

    /// Get the total number of rows changed since the connection was opened
    pub fn total_changes(&self) -> i64 {
        unsafe { ffi::sqlite3_total_changes64(self.db) }
    }

    /// Set the busy timeout in milliseconds
    pub fn busy_timeout(&self, ms: i32) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_busy_timeout(self.db, ms) };
        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(self.db) });
        }
        Ok(())
    }

    /// Enable or disable extended result codes
    pub fn extended_result_codes(&self, enable: bool) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_extended_result_codes(self.db, enable as i32) };
        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(self.db) });
        }
        Ok(())
    }

    /// Interrupt any pending database operation
    pub fn interrupt(&self) {
        unsafe { ffi::sqlite3_interrupt(self.db) }
    }

    /// Check if an interrupt is pending
    pub fn is_interrupted(&self) -> bool {
        unsafe { ffi::sqlite3_is_interrupted(self.db) != 0 }
    }

    /// Get the database filename
    pub fn db_filename(&self, db_name: &str) -> Option<&str> {
        let c_name = CString::new(db_name).ok()?;
        let ptr = unsafe { ffi::sqlite3_db_filename(self.db, c_name.as_ptr()) };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(ptr).to_str().ok()? })
        }
    }

    /// Check if the database is read-only
    pub fn is_readonly(&self, db_name: &str) -> Option<bool> {
        let c_name = CString::new(db_name).ok()?;
        let result = unsafe { ffi::sqlite3_db_readonly(self.db, c_name.as_ptr()) };
        match result {
            -1 => None, // Unknown database
            0 => Some(false),
            _ => Some(true),
        }
    }

    /// Get the raw database handle (for advanced use)
    ///
    /// # Safety
    ///
    /// The caller must ensure that the returned pointer is not used after
    /// the Connection is dropped.
    pub unsafe fn handle(&self) -> *mut ffi::sqlite3 {
        self.db
    }

    /// Get the raw database pointer
    ///
    /// This is primarily used for statement caching. The pointer should
    /// not be used after the Connection is dropped.
    pub fn as_ptr(&self) -> *mut ffi::sqlite3 {
        self.db
    }

    /// Close the database connection explicitly
    ///
    /// This is called automatically on drop, but can be called explicitly
    /// if you want to handle errors.
    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        if !self.db.is_null() {
            let rc = unsafe { ffi::sqlite3_close(self.db) };
            if rc != ffi::SQLITE_OK {
                // Don't set db to null yet - caller might want to try again
                return Err(Error::new(rc));
            }
            self.db = ptr::null_mut();
        }
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Use close_v2 which always succeeds (defers if busy)
        if !self.db.is_null() {
            unsafe { ffi::sqlite3_close_v2(self.db) };
            self.db = ptr::null_mut();
        }
    }
}

/// A thread-safe SQLite database connection
///
/// This connection type can be safely shared across threads using `Arc<ThreadSafeConnection>`
/// without requiring a Mutex. It achieves this by:
///
/// 1. SQLite is compiled with SQLITE_THREADSAFE=1 (serialized mode) by default,
///    which makes it internally thread-safe
/// 2. Using statement-per-query pattern - each query creates its own statement
///    that is used and dropped within the same call
/// 3. Returning only owned data (no borrowed references that could escape)
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use pasyn_sqlite_core::ThreadSafeConnection;
///
/// let conn = Arc::new(ThreadSafeConnection::open("mydb.sqlite").unwrap());
///
/// // Can be safely cloned and used from multiple threads
/// let conn2 = conn.clone();
/// std::thread::spawn(move || {
///     let rows = conn2.query_fetchall("SELECT * FROM users", &[]).unwrap();
/// });
/// ```
pub struct ThreadSafeConnection {
    db: *mut ffi::sqlite3,
}

// SAFETY: SQLite is compiled with SQLITE_THREADSAFE=1 (serialized mode) by default,
// which makes all SQLite operations thread-safe. Combined with the statement-per-query
// pattern (no statement state escapes method boundaries), this is safe for concurrent access.
unsafe impl Send for ThreadSafeConnection {}
unsafe impl Sync for ThreadSafeConnection {}

impl ThreadSafeConnection {
    /// Open a database connection (thread-safe)
    ///
    /// SQLite's default serialized mode handles thread safety internally.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_flags(path, OpenFlags::default_readwrite())
    }

    /// Open a database connection with specific flags
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy();
        let c_path = CString::new(path_str.as_bytes())?;

        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        let rc =
            unsafe { ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags.bits(), ptr::null()) };

        if rc != ffi::SQLITE_OK {
            if !db.is_null() {
                let err = unsafe { Error::from_db(db) };
                unsafe { ffi::sqlite3_close(db) };
                return Err(err);
            }
            return Err(Error::new(rc));
        }

        Ok(ThreadSafeConnection { db })
    }

    /// Open a read-only database connection (thread-safe)
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_flags(path, OpenFlags::READONLY)
    }

    /// Open an in-memory database (thread-safe)
    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Execute a SQL statement that doesn't return any data
    ///
    /// Returns the number of rows changed.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<usize> {
        let mut stmt = Statement::prepare(self.db, sql)?;
        stmt.bind_all(params.iter().cloned())?;

        loop {
            let rc = unsafe { ffi::sqlite3_step(stmt.as_ptr()) };
            match rc {
                ffi::SQLITE_DONE => break,
                ffi::SQLITE_ROW => continue,
                _ => return Err(unsafe { Error::from_db(self.db) }),
            }
        }

        Ok(unsafe { ffi::sqlite3_changes(self.db) as usize })
    }

    /// Execute multiple SQL statements (batch)
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let c_sql = CString::new(sql)?;
        let mut errmsg: *mut std::os::raw::c_char = ptr::null_mut();

        let rc = unsafe {
            ffi::sqlite3_exec(self.db, c_sql.as_ptr(), None, ptr::null_mut(), &mut errmsg)
        };

        if rc != ffi::SQLITE_OK {
            let message = if !errmsg.is_null() {
                let msg = unsafe { CStr::from_ptr(errmsg) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { ffi::sqlite3_free(errmsg as *mut _) };
                msg
            } else {
                "Unknown error".to_string()
            };

            return Err(Error::with_message(ErrorCode::from_raw(rc), message));
        }

        Ok(())
    }

    /// Query and fetch all results as owned data
    ///
    /// This is the primary query method for thread-safe access. It creates a
    /// statement, executes it, collects all rows into owned `Vec<Value>` vectors,
    /// and returns them. No borrowed references escape this method.
    pub fn query_fetchall(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        let mut stmt = Statement::prepare(self.db, sql)?;
        stmt.bind_all(params.iter().cloned())?;

        let mut rows = Vec::new();
        while stmt.step()? {
            rows.push(stmt.columns());
        }
        Ok(rows)
    }

    /// Query a single row and return owned values
    pub fn query_row(&self, sql: &str, params: &[Value]) -> Result<Option<Vec<Value>>> {
        let mut stmt = Statement::prepare(self.db, sql)?;
        stmt.bind_all(params.iter().cloned())?;

        if stmt.step()? {
            Ok(Some(stmt.columns()))
        } else {
            Ok(None)
        }
    }

    /// Query with custom row conversion
    ///
    /// This method allows converting rows directly without intermediate Value allocation.
    /// The closure receives a reference to the Statement for each row and should return
    /// the converted value.
    pub fn query_map<T, F>(&self, sql: &str, params: &[Value], mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(&Statement) -> T,
    {
        let mut stmt = Statement::prepare(self.db, sql)?;
        stmt.bind_all(params.iter().cloned())?;

        let mut results = Vec::new();
        while stmt.step()? {
            results.push(f(&stmt));
        }
        Ok(results)
    }

    /// Query single row with custom conversion
    pub fn query_row_map<T, F>(&self, sql: &str, params: &[Value], f: F) -> Result<Option<T>>
    where
        F: FnOnce(&Statement) -> T,
    {
        let mut stmt = Statement::prepare(self.db, sql)?;
        stmt.bind_all(params.iter().cloned())?;

        if stmt.step()? {
            Ok(Some(f(&stmt)))
        } else {
            Ok(None)
        }
    }

    /// Prepare a statement for manual iteration
    ///
    /// This is useful when you need more control over statement execution
    /// or want to do custom conversion.
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        Statement::prepare(self.db, sql)
    }

    /// Query using cached prepared statement (faster for repeated queries)
    ///
    /// This method uses a thread-local statement cache to avoid re-preparing
    /// statements that have been used before. The cache is keyed by SQL text.
    ///
    /// For best performance, use this for queries that are executed frequently
    /// with different parameters.
    pub fn query_cached<F, T>(&self, sql: &str, params: &[Value], mut f: F) -> Result<Vec<T>>
    where
        F: FnMut(*mut ffi::sqlite3_stmt) -> T,
    {
        let stmt = crate::cache::get_cached_statement(self.db, sql)?;

        // Bind parameters
        for (i, param) in params.iter().enumerate() {
            let idx = (i + 1) as std::os::raw::c_int;
            let rc = match param {
                Value::Null => unsafe { ffi::sqlite3_bind_null(stmt, idx) },
                Value::Integer(v) => unsafe { ffi::sqlite3_bind_int64(stmt, idx, *v) },
                Value::Real(v) => unsafe { ffi::sqlite3_bind_double(stmt, idx, *v) },
                Value::Text(s) => unsafe {
                    ffi::sqlite3_bind_text(
                        stmt,
                        idx,
                        s.as_ptr() as *const _,
                        s.len() as std::os::raw::c_int,
                        ffi::sqlite_transient(),
                    )
                },
                Value::Blob(b) => unsafe {
                    ffi::sqlite3_bind_blob(
                        stmt,
                        idx,
                        b.as_ptr() as *const _,
                        b.len() as std::os::raw::c_int,
                        ffi::sqlite_transient(),
                    )
                },
            };
            if rc != ffi::SQLITE_OK {
                return Err(unsafe { Error::from_db(self.db) });
            }
        }

        // Execute and collect results
        let mut results = Vec::new();
        loop {
            let rc = unsafe { ffi::sqlite3_step(stmt) };
            match rc {
                ffi::SQLITE_ROW => results.push(f(stmt)),
                ffi::SQLITE_DONE => break,
                _ => return Err(unsafe { Error::from_db(self.db) }),
            }
        }

        Ok(results)
    }

    /// Query single row using cached prepared statement
    ///
    /// Similar to query_cached but stops after the first row.
    pub fn query_cached_one<F, T>(&self, sql: &str, params: &[Value], f: F) -> Result<Option<T>>
    where
        F: FnOnce(*mut ffi::sqlite3_stmt) -> T,
    {
        let stmt = crate::cache::get_cached_statement(self.db, sql)?;

        // Bind parameters
        for (i, param) in params.iter().enumerate() {
            let idx = (i + 1) as std::os::raw::c_int;
            let rc = match param {
                Value::Null => unsafe { ffi::sqlite3_bind_null(stmt, idx) },
                Value::Integer(v) => unsafe { ffi::sqlite3_bind_int64(stmt, idx, *v) },
                Value::Real(v) => unsafe { ffi::sqlite3_bind_double(stmt, idx, *v) },
                Value::Text(s) => unsafe {
                    ffi::sqlite3_bind_text(
                        stmt,
                        idx,
                        s.as_ptr() as *const _,
                        s.len() as std::os::raw::c_int,
                        ffi::sqlite_transient(),
                    )
                },
                Value::Blob(b) => unsafe {
                    ffi::sqlite3_bind_blob(
                        stmt,
                        idx,
                        b.as_ptr() as *const _,
                        b.len() as std::os::raw::c_int,
                        ffi::sqlite_transient(),
                    )
                },
            };
            if rc != ffi::SQLITE_OK {
                return Err(unsafe { Error::from_db(self.db) });
            }
        }

        // Execute and return first result only
        let rc = unsafe { ffi::sqlite3_step(stmt) };
        match rc {
            ffi::SQLITE_ROW => Ok(Some(f(stmt))),
            ffi::SQLITE_DONE => Ok(None),
            _ => Err(unsafe { Error::from_db(self.db) }),
        }
    }

    /// Get the raw database pointer (for cache key)
    pub fn as_ptr(&self) -> *mut ffi::sqlite3 {
        self.db
    }

    /// Execute the same SQL statement with multiple parameter sets
    pub fn execute_many(&self, sql: &str, params_batch: &[Vec<Value>]) -> Result<usize> {
        let mut stmt = Statement::prepare(self.db, sql)?;
        let mut total_changes = 0usize;

        for params in params_batch {
            stmt.reset()?;
            stmt.clear_bindings()?;
            stmt.bind_all(params.iter().cloned())?;

            loop {
                let rc = unsafe { ffi::sqlite3_step(stmt.as_ptr()) };
                match rc {
                    ffi::SQLITE_DONE => break,
                    ffi::SQLITE_ROW => continue,
                    _ => return Err(unsafe { Error::from_db(self.db) }),
                }
            }
            total_changes += unsafe { ffi::sqlite3_changes(self.db) as usize };
        }

        Ok(total_changes)
    }

    /// Get the rowid of the last inserted row
    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db) }
    }

    /// Get the number of rows changed by the last statement
    pub fn changes(&self) -> i64 {
        unsafe { ffi::sqlite3_changes64(self.db) }
    }

    /// Check if the connection is in autocommit mode
    pub fn is_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.db) != 0 }
    }

    /// Set the busy timeout in milliseconds
    pub fn busy_timeout(&self, ms: i32) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_busy_timeout(self.db, ms) };
        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(self.db) });
        }
        Ok(())
    }

    /// Get the raw database handle (for advanced use)
    ///
    /// # Safety
    ///
    /// The caller must ensure that the returned pointer is not used after
    /// the connection is dropped, and must respect SQLite's threading rules.
    pub unsafe fn handle(&self) -> *mut ffi::sqlite3 {
        self.db
    }
}

impl Drop for ThreadSafeConnection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            // Clear statement cache before closing connection
            // (cached statements hold references to this db)
            crate::cache::clear_cache_for_db(self.db);
            unsafe { ffi::sqlite3_close_v2(self.db) };
            self.db = ptr::null_mut();
        }
    }
}

/// A database transaction
///
/// Transactions are automatically rolled back on drop unless committed.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    committed: bool,
}

impl<'conn> Transaction<'conn> {
    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        self.conn.execute_batch("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction
    pub fn rollback(mut self) -> Result<()> {
        self.conn.execute_batch("ROLLBACK")?;
        self.committed = true; // Prevent double rollback on drop
        Ok(())
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &Connection {
        self.conn
    }
}

impl<'conn> Drop for Transaction<'conn> {
    fn drop(&mut self) {
        if !self.committed {
            // Best effort rollback on drop
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

/// A row from a query result
pub struct Row<'stmt> {
    stmt: &'stmt Statement,
}

impl<'stmt> Row<'stmt> {
    fn new(stmt: &'stmt Statement) -> Self {
        Row { stmt }
    }

    /// Get the number of columns in the row
    pub fn column_count(&self) -> usize {
        self.stmt.column_count()
    }

    /// Get a column value by index
    pub fn get<T: FromColumn>(&self, index: usize) -> Result<T> {
        T::from_column(self.stmt, index)
    }

    /// Get a column name by index
    pub fn column_name(&self, index: usize) -> Option<&str> {
        self.stmt.column_name(index)
    }
}

/// Trait for types that can be extracted from a column
pub trait FromColumn: Sized {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self>;
}

impl FromColumn for i32 {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        Ok(stmt.column_int(index))
    }
}

impl FromColumn for i64 {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        Ok(stmt.column_int64(index))
    }
}

impl FromColumn for f64 {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        Ok(stmt.column_double(index))
    }
}

impl FromColumn for String {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        stmt.column_text(index).map(|s| s.to_string())
    }
}

impl FromColumn for Vec<u8> {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        Ok(stmt.column_blob(index).to_vec())
    }
}

impl FromColumn for Value {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        Ok(stmt.column_value(index))
    }
}

impl<T: FromColumn> FromColumn for Option<T> {
    fn from_column(stmt: &Statement, index: usize) -> Result<Self> {
        if stmt.column_is_null(index) {
            Ok(None)
        } else {
            Ok(Some(T::from_column(stmt, index)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NO_PARAMS: [Value; 0] = [];

    #[test]
    fn test_open_in_memory() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(conn.is_autocommit());
    }

    #[test]
    fn test_execute() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
            NO_PARAMS,
        )
        .unwrap();
        let changes = conn
            .execute("INSERT INTO test (name) VALUES (?1)", ["Alice"])
            .unwrap();
        assert_eq!(changes, 1);
        assert_eq!(conn.last_insert_rowid(), 1);
    }

    #[test]
    fn test_query() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)", NO_PARAMS)
            .unwrap();
        conn.execute("INSERT INTO test VALUES (1, 'Alice')", NO_PARAMS)
            .unwrap();

        let name: Option<String> = conn
            .query_row("SELECT name FROM test WHERE id = ?1", [1], |row| row.get(0))
            .unwrap();
        assert_eq!(name, Some("Alice".to_string()));
    }

    #[test]
    fn test_transaction() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE test (id INTEGER)", NO_PARAMS)
            .unwrap();

        // Test commit
        {
            let tx = conn.begin_transaction().unwrap();
            tx.connection()
                .execute("INSERT INTO test VALUES (1)", NO_PARAMS)
                .unwrap();
            tx.commit().unwrap();
        }

        // Test rollback
        {
            let tx = conn.begin_transaction().unwrap();
            tx.connection()
                .execute("INSERT INTO test VALUES (2)", NO_PARAMS)
                .unwrap();
            tx.rollback().unwrap();
        }

        // Verify
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", NO_PARAMS, |row| row.get(0))
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_thread_safe_connection_basic() {
        let conn = ThreadSafeConnection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")
            .unwrap();
        conn.execute("INSERT INTO test VALUES (?, ?)", &[Value::Integer(1), Value::Text("Alice".to_string())])
            .unwrap();

        let rows = conn.query_fetchall("SELECT * FROM test", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[0][1], Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_thread_safe_connection_concurrent() {
        use std::sync::Arc;
        use std::thread;

        // Create a file-based database for concurrent test
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_concurrent.db");

        // Clean up any existing file
        let _ = std::fs::remove_file(&db_path);

        let conn = Arc::new(ThreadSafeConnection::open(&db_path).unwrap());
        conn.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();

        // Insert some data
        for i in 0..100 {
            conn.execute(
                "INSERT INTO test (value) VALUES (?)",
                &[Value::Text(format!("value_{}", i))],
            )
            .unwrap();
        }

        // Spawn multiple threads that all read concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let conn = conn.clone();
                thread::spawn(move || {
                    // Each thread does multiple reads
                    for _ in 0..10 {
                        let rows = conn.query_fetchall("SELECT * FROM test", &[]).unwrap();
                        assert_eq!(rows.len(), 100);
                    }
                })
            })
            .collect();

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Clean up
        drop(conn);
        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn test_thread_safe_connection_execute_many() {
        let conn = ThreadSafeConnection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")
            .unwrap();

        let params: Vec<Vec<Value>> = (0..100)
            .map(|i| vec![Value::Integer(i), Value::Text(format!("name_{}", i))])
            .collect();

        let changed = conn.execute_many("INSERT INTO test VALUES (?, ?)", &params).unwrap();
        assert_eq!(changed, 100);

        let rows = conn.query_fetchall("SELECT COUNT(*) FROM test", &[]).unwrap();
        assert_eq!(rows[0][0], Value::Integer(100));
    }
}
