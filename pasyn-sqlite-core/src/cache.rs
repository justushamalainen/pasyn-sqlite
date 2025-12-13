//! Thread-local statement cache using DJB2 hash (APSW-inspired)
//!
//! This module provides a high-performance statement cache that reuses prepared
//! statements across multiple queries with the same SQL text. Uses DJB2 hash
//! algorithm for fast SQL string hashing, following APSW's proven approach.
//!
//! Key optimizations from APSW:
//! - DJB2 hash for fast SQL string hashing
//! - Array-based cache with linear probing
//! - Statement reuse tracking via sqlite3_stmt_status
//! - Thread-local storage to avoid locking

use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr;

use crate::error::{Error, Result};
use crate::ffi;

/// Maximum number of statements to cache per connection per thread
const MAX_CACHE_SIZE: usize = 100;

/// DJB2 hash algorithm - same as APSW uses for fast SQL string hashing
///
/// This is a simple and fast hash function that works well for SQL strings.
/// Initial value of 5381 and multiplier of 33 are the classic DJB2 constants.
#[inline]
fn djb2_hash(sql: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for &byte in sql {
        // hash * 33 + byte
        hash = hash.wrapping_mul(33).wrapping_add(byte as u32);
    }
    hash
}

/// A cached prepared statement with metadata for LRU eviction
struct CachedStatement {
    stmt: *mut ffi::sqlite3_stmt,
    /// Original SQL text for verification
    sql: String,
    /// Hash of the SQL for quick comparison
    hash: u32,
    /// Access counter for LRU eviction
    last_access: u64,
}

impl CachedStatement {
    /// Check if this statement can be reused
    ///
    /// A statement can be reused if it hasn't been run yet (after reset),
    /// following APSW's pattern of checking SQLITE_STMTSTATUS_RUN.
    #[inline]
    fn can_reuse(&self) -> bool {
        // Check if statement has been run since last reset
        // A value of 0 means it hasn't been stepped since the last reset
        let run_count =
            unsafe { ffi::sqlite3_stmt_status(self.stmt, ffi::SQLITE_STMTSTATUS_RUN, 0) };
        run_count == 0
    }
}

/// Thread-local statement cache for a single database connection
///
/// Uses DJB2 hash for fast lookup and linear probing for collision resolution.
struct ConnectionCache {
    /// Cached statements - array for cache-friendly access
    statements: Vec<Option<CachedStatement>>,
    /// Quick lookup by hash -> index in statements array
    hash_index: HashMap<u32, Vec<usize>>,
    /// Counter for LRU eviction
    access_counter: u64,
    /// Number of active entries
    count: usize,
}

impl ConnectionCache {
    fn new() -> Self {
        ConnectionCache {
            statements: Vec::with_capacity(MAX_CACHE_SIZE),
            hash_index: HashMap::new(),
            access_counter: 0,
            count: 0,
        }
    }

    /// Get or prepare a statement, returning the raw pointer
    ///
    /// The returned statement is already reset and ready for binding.
    fn get_or_prepare(
        &mut self,
        db: *mut ffi::sqlite3,
        sql: &str,
    ) -> Result<*mut ffi::sqlite3_stmt> {
        self.access_counter += 1;
        let sql_bytes = sql.as_bytes();
        let hash = djb2_hash(sql_bytes);

        // Fast path: check hash index for potential matches
        if let Some(indices) = self.hash_index.get(&hash) {
            for &idx in indices {
                if let Some(ref mut cached) = self.statements.get_mut(idx).and_then(|s| s.as_mut())
                {
                    // Verify SQL matches (hash collision check)
                    if cached.sql == sql {
                        // Found matching statement - reset and reuse
                        let rc = unsafe { ffi::sqlite3_reset(cached.stmt) };
                        if rc != ffi::SQLITE_OK {
                            // Statement is invalid, remove and prepare fresh
                            self.remove_at_index(idx);
                            return self.prepare_and_cache(db, sql, hash);
                        }

                        // Clear bindings for fresh use
                        unsafe { ffi::sqlite3_clear_bindings(cached.stmt) };
                        cached.last_access = self.access_counter;
                        return Ok(cached.stmt);
                    }
                }
            }
        }

        // Cache miss - prepare new statement
        self.prepare_and_cache(db, sql, hash)
    }

    fn prepare_and_cache(
        &mut self,
        db: *mut ffi::sqlite3,
        sql: &str,
        hash: u32,
    ) -> Result<*mut ffi::sqlite3_stmt> {
        // Evict if cache is full
        if self.count >= MAX_CACHE_SIZE {
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

        // Find empty slot or extend
        let idx = self.find_empty_slot();

        let cached = CachedStatement {
            stmt,
            sql: sql.to_string(),
            hash,
            last_access: self.access_counter,
        };

        // Store in array
        if idx < self.statements.len() {
            self.statements[idx] = Some(cached);
        } else {
            self.statements.push(Some(cached));
        }

        // Update hash index
        self.hash_index.entry(hash).or_default().push(idx);
        self.count += 1;

        Ok(stmt)
    }

    fn find_empty_slot(&self) -> usize {
        for (i, slot) in self.statements.iter().enumerate() {
            if slot.is_none() {
                return i;
            }
        }
        self.statements.len()
    }

    fn remove_at_index(&mut self, idx: usize) {
        if let Some(Some(cached)) = self.statements.get(idx) {
            let hash = cached.hash;
            let stmt = cached.stmt;

            // Remove from hash index
            if let Some(indices) = self.hash_index.get_mut(&hash) {
                indices.retain(|&i| i != idx);
                if indices.is_empty() {
                    self.hash_index.remove(&hash);
                }
            }

            // Finalize statement
            unsafe { ffi::sqlite3_finalize(stmt) };
            self.statements[idx] = None;
            self.count -= 1;
        }
    }

    fn evict_lru(&mut self) {
        // Find the least recently used statement
        let mut min_access = u64::MAX;
        let mut min_idx = None;

        for (i, slot) in self.statements.iter().enumerate() {
            if let Some(ref cached) = slot {
                if cached.last_access < min_access {
                    min_access = cached.last_access;
                    min_idx = Some(i);
                }
            }
        }

        if let Some(idx) = min_idx {
            self.remove_at_index(idx);
        }
    }

    fn clear(&mut self) {
        for slot in self.statements.iter_mut() {
            if let Some(cached) = slot.take() {
                unsafe { ffi::sqlite3_finalize(cached.stmt) };
            }
        }
        self.hash_index.clear();
        self.count = 0;
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
/// Uses DJB2 hash for fast lookup, following APSW's optimization pattern.
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

/// Get cache statistics for debugging
pub fn cache_stats(db: *mut ffi::sqlite3) -> Option<(usize, u64)> {
    CACHE.with(|cache| {
        let cache = cache.borrow();
        let db_key = db as usize;
        cache
            .get(&db_key)
            .map(|c| (c.count, c.access_counter))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;

    #[test]
    fn test_djb2_hash() {
        // Basic hash test
        let h1 = djb2_hash(b"SELECT * FROM users");
        let h2 = djb2_hash(b"SELECT * FROM users");
        let h3 = djb2_hash(b"SELECT * FROM orders");

        assert_eq!(h1, h2, "Same SQL should produce same hash");
        assert_ne!(h1, h3, "Different SQL should produce different hash");
    }

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

    #[test]
    fn test_cache_stats() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER)").unwrap();

        let db = conn.as_ptr();

        // Initially no stats
        let stats = cache_stats(db);
        assert!(stats.is_none() || stats == Some((0, 0)));

        // After caching some statements
        get_cached_statement(db, "SELECT 1").unwrap();
        get_cached_statement(db, "SELECT 2").unwrap();
        get_cached_statement(db, "SELECT 1").unwrap(); // Cache hit

        let (count, accesses) = cache_stats(db).unwrap();
        assert_eq!(count, 2); // Two unique statements
        assert_eq!(accesses, 3); // Three accesses

        clear_cache_for_db(db);
    }
}
