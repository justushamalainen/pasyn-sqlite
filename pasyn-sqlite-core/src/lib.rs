//! pasyn-sqlite-core - SQLite bindings for pasyn
//!
//! This crate provides Rust bindings to SQLite with support for Python via PyO3.
//!
//! # Features
//!
//! - **Safe Rust API**: Idiomatic Rust wrappers around SQLite
//! - **Direct FFI bindings**: Low-level access to SQLite C API when needed
//! - **Python bindings**: Optional PyO3 bindings for Python integration
//! - **Thread-safe**: Designed for concurrent access
//!
//! # Quick Start
//!
//! ```
//! use pasyn_sqlite_core::{Connection, Value};
//!
//! // Open a database
//! let conn = Connection::open_in_memory()?;
//!
//! // Create a table
//! conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
//!
//! // Insert data
//! conn.execute("INSERT INTO users (name) VALUES (?1)", ["Alice"])?;
//!
//! // Query data
//! let name: Option<String> = conn.query_row(
//!     "SELECT name FROM users WHERE id = ?1",
//!     [1],
//!     |row| row.get(0)
//! )?;
//!
//! assert_eq!(name, Some("Alice".to_string()));
//! # Ok::<(), pasyn_sqlite_core::Error>(())
//! ```
//!
//! # Transactions
//!
//! ```
//! use pasyn_sqlite_core::Connection;
//!
//! let conn = Connection::open_in_memory()?;
//! conn.execute_batch("CREATE TABLE test (value INTEGER)")?;
//!
//! // Transaction that commits
//! {
//!     let tx = conn.begin_transaction()?;
//!     tx.connection().execute_batch("INSERT INTO test VALUES (1)")?;
//!     tx.commit()?;
//! }
//!
//! // Transaction that rolls back on drop
//! {
//!     let tx = conn.begin_transaction()?;
//!     tx.connection().execute_batch("INSERT INTO test VALUES (2)")?;
//!     // tx is dropped here, rolling back
//! }
//!
//! # Ok::<(), pasyn_sqlite_core::Error>(())
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod connection;
pub mod error;
pub mod ffi;
pub mod statement;
pub mod value;

// Writer server architecture
pub mod protocol;
pub mod server;
pub mod client;

#[cfg(feature = "python")]
pub mod python;

// Re-exports
pub use connection::{Connection, FromColumn, OpenFlags, Row, Transaction};
pub use error::{Error, ErrorCode, Result};
pub use statement::{ColumnType, RowRef, Rows, Statement};
pub use value::{Params, Value};

// Server/client re-exports
pub use server::{ServerConfig, ServerHandle, WriterServer};
pub use client::MultiplexedClient;

/// Get the SQLite library version string
pub fn sqlite_version() -> &'static str {
    use std::ffi::CStr;
    unsafe {
        let ptr = ffi::sqlite3_libversion();
        CStr::from_ptr(ptr).to_str().unwrap_or("unknown")
    }
}

/// Get the SQLite library version number
///
/// The version number is in the format XYYYZZZZ where
/// X is the major version, YYY is the minor version,
/// and ZZZZ is the release.
pub fn sqlite_version_number() -> i32 {
    unsafe { ffi::sqlite3_libversion_number() }
}

/// Check if SQLite was compiled with thread-safety
pub fn sqlite_threadsafe() -> bool {
    unsafe { ffi::sqlite3_threadsafe() != 0 }
}

/// Get memory currently in use by SQLite
pub fn memory_used() -> i64 {
    unsafe { ffi::sqlite3_memory_used() }
}

/// Get the high-water mark for memory usage
pub fn memory_highwater(reset: bool) -> i64 {
    unsafe { ffi::sqlite3_memory_highwater(reset as i32) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let version = sqlite_version();
        assert!(version.starts_with("3."), "Version should start with 3.");
        println!("SQLite version: {}", version);
    }

    #[test]
    fn test_version_number() {
        let version = sqlite_version_number();
        assert!(version >= 3000000, "Version should be at least 3.0.0");
        println!("SQLite version number: {}", version);
    }

    #[test]
    fn test_threadsafe() {
        assert!(sqlite_threadsafe(), "SQLite should be thread-safe");
    }

    #[test]
    fn test_memory() {
        // Just verify the functions work
        let _used = memory_used();
        let _high = memory_highwater(false);
    }

    const NO_PARAMS: [Value; 0] = [];

    #[test]
    fn test_basic_operations() {
        let conn = Connection::open_in_memory().unwrap();

        // Create table
        conn.execute_batch(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)",
        )
        .unwrap();

        // Insert
        conn.execute("INSERT INTO test (name, value) VALUES (?1, ?2)", ["foo", "1.5"])
            .unwrap();
        conn.execute(
            "INSERT INTO test (name, value) VALUES (?1, ?2)",
            [Value::Text("bar".to_string()), Value::Real(2.5)],
        )
        .unwrap();

        // Query
        let mut stmt = conn.prepare("SELECT name, value FROM test ORDER BY id").unwrap();

        assert!(stmt.step().unwrap());
        assert_eq!(stmt.column_text(0).unwrap(), "foo");

        assert!(stmt.step().unwrap());
        assert_eq!(stmt.column_text(0).unwrap(), "bar");
        assert!((stmt.column_double(1) - 2.5).abs() < 0.001);

        assert!(!stmt.step().unwrap()); // No more rows
    }

    #[test]
    fn test_transaction_commit() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER)").unwrap();

        {
            let tx = conn.begin_transaction().unwrap();
            tx.connection()
                .execute_batch("INSERT INTO test VALUES (1)")
                .unwrap();
            tx.commit().unwrap();
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", NO_PARAMS, |row| row.get(0))
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_transaction_rollback() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER)").unwrap();

        {
            let tx = conn.begin_transaction().unwrap();
            tx.connection()
                .execute_batch("INSERT INTO test VALUES (1)")
                .unwrap();
            // Dropped without commit - should rollback
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", NO_PARAMS, |row| row.get(0))
            .unwrap()
            .unwrap();
        assert_eq!(count, 0);
    }
}
