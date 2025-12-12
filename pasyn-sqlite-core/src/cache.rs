//! Thread-local statement cache for improved query performance
//!
//! This module provides a statement cache that reuses prepared statements
//! across multiple queries with the same SQL text. Each thread maintains
//! its own cache, avoiding any locking overhead.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr;

use crate::error::{Error, Result};
use crate::ffi;

/// Maximum number of statements to cache per connection per thread
const MAX_CACHE_SIZE: usize = 100;

/// A cached prepared statement
struct CachedStatement {
    stmt: *mut ffi::sqlite3_stmt,
    /// Number of times this statement has been used (for LRU eviction)
    use_count: u64,
}

/// Thread-local statement cache for a single database connection
struct ConnectionCache {
    /// Cached statements keyed by SQL text
    statements: HashMap<String, CachedStatement>,
    /// Counter for LRU eviction
    access_counter: u64,
}

impl ConnectionCache {
    fn new() -> Self {
        ConnectionCache {
            statements: HashMap::new(),
            access_counter: 0,
        }
    }

    /// Get or prepare a statement, returning the raw pointer
    ///
    /// The returned statement is already reset and ready for binding.
    fn get_or_prepare(&mut self, db: *mut ffi::sqlite3, sql: &str) -> Result<*mut ffi::sqlite3_stmt> {
        self.access_counter += 1;

        // Check cache first
        if let Some(cached) = self.statements.get_mut(sql) {
            // Reset statement for reuse
            let rc = unsafe { ffi::sqlite3_reset(cached.stmt) };
            if rc != ffi::SQLITE_OK {
                // Statement is invalid, remove from cache and prepare fresh
                let stmt = cached.stmt;
                self.statements.remove(sql);
                unsafe { ffi::sqlite3_finalize(stmt) };
                return self.prepare_and_cache(db, sql);
            }

            // Clear bindings
            unsafe { ffi::sqlite3_clear_bindings(cached.stmt) };

            cached.use_count = self.access_counter;
            return Ok(cached.stmt);
        }

        // Cache miss - prepare new statement
        self.prepare_and_cache(db, sql)
    }

    fn prepare_and_cache(&mut self, db: *mut ffi::sqlite3, sql: &str) -> Result<*mut ffi::sqlite3_stmt> {
        // Evict if cache is full
        if self.statements.len() >= MAX_CACHE_SIZE {
            self.evict_lru();
        }

        // Prepare new statement
        let c_sql = std::ffi::CString::new(sql)?;
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();

        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                db,
                c_sql.as_ptr(),
                c_sql.as_bytes().len() as std::os::raw::c_int,
                &mut stmt,
                ptr::null_mut(),
            )
        };

        if rc != ffi::SQLITE_OK {
            return Err(unsafe { Error::from_db(db) });
        }

        // Cache the statement
        self.statements.insert(
            sql.to_string(),
            CachedStatement {
                stmt,
                use_count: self.access_counter,
            },
        );

        Ok(stmt)
    }

    fn evict_lru(&mut self) {
        // Find the least recently used statement
        if let Some((sql, _)) = self
            .statements
            .iter()
            .min_by_key(|(_, cached)| cached.use_count)
            .map(|(k, v)| (k.clone(), v.stmt))
        {
            if let Some(cached) = self.statements.remove(&sql) {
                unsafe { ffi::sqlite3_finalize(cached.stmt) };
            }
        }
    }

    fn clear(&mut self) {
        for (_, cached) in self.statements.drain() {
            unsafe { ffi::sqlite3_finalize(cached.stmt) };
        }
    }
}

impl Drop for ConnectionCache {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Global thread-local cache, keyed by database pointer
thread_local! {
    static CACHE: RefCell<HashMap<usize, ConnectionCache>> = RefCell::new(HashMap::new());
}

/// Get or prepare a cached statement for the given database and SQL
///
/// # Safety
///
/// The caller must ensure:
/// - The database pointer is valid
/// - The returned statement pointer is not used after the database is closed
/// - The statement is reset before the next call with the same SQL
pub fn get_cached_statement(db: *mut ffi::sqlite3, sql: &str) -> Result<*mut ffi::sqlite3_stmt> {
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let db_key = db as usize;

        let conn_cache = cache.entry(db_key).or_insert_with(ConnectionCache::new);
        conn_cache.get_or_prepare(db, sql)
    })
}

/// Clear the cache for a specific database connection
///
/// Should be called when a connection is closed.
pub fn clear_cache_for_db(db: *mut ffi::sqlite3) {
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let db_key = db as usize;
        if let Some(mut conn_cache) = cache.remove(&db_key) {
            conn_cache.clear();
        }
    });
}

/// Clear all cached statements for the current thread
pub fn clear_all_caches() {
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (_, mut conn_cache) in cache.drain() {
            conn_cache.clear();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;

    #[test]
    fn test_statement_cache_basic() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")
            .unwrap();

        let db = conn.as_ptr();

        // First access - cache miss
        let stmt1 = get_cached_statement(db, "SELECT * FROM test WHERE id = ?").unwrap();
        assert!(!stmt1.is_null());

        // Second access - cache hit (same pointer)
        let stmt2 = get_cached_statement(db, "SELECT * FROM test WHERE id = ?").unwrap();
        assert_eq!(stmt1, stmt2);

        // Different SQL - cache miss
        let stmt3 = get_cached_statement(db, "SELECT * FROM test WHERE name = ?").unwrap();
        assert_ne!(stmt1, stmt3);

        clear_cache_for_db(db);
    }

    #[test]
    fn test_statement_cache_reuse() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
            .unwrap();
        conn.execute_batch("INSERT INTO test VALUES (1, 100), (2, 200)")
            .unwrap();

        let db = conn.as_ptr();
        let sql = "SELECT value FROM test WHERE id = ?";

        // First query
        let stmt = get_cached_statement(db, sql).unwrap();
        unsafe {
            ffi::sqlite3_bind_int(stmt, 1, 1);
            assert_eq!(ffi::sqlite3_step(stmt), ffi::SQLITE_ROW);
            assert_eq!(ffi::sqlite3_column_int(stmt, 0), 100);
        }

        // Reuse for second query (automatically reset by get_cached_statement)
        let stmt2 = get_cached_statement(db, sql).unwrap();
        assert_eq!(stmt, stmt2);
        unsafe {
            ffi::sqlite3_bind_int(stmt2, 1, 2);
            assert_eq!(ffi::sqlite3_step(stmt2), ffi::SQLITE_ROW);
            assert_eq!(ffi::sqlite3_column_int(stmt2, 0), 200);
        }

        clear_cache_for_db(db);
    }
}
