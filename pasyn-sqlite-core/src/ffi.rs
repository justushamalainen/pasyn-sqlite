//! Raw FFI bindings to SQLite3 C library
//!
//! This module contains the low-level C bindings to SQLite.
//! These are unsafe and should be used through the safe wrappers in the parent module.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use std::os::raw::{c_char, c_double, c_int, c_uchar, c_void};

/// SQLite 64-bit integer type
pub type sqlite3_int64 = i64;
pub type sqlite3_uint64 = u64;

/// Opaque database connection handle
#[repr(C)]
pub struct sqlite3 {
    _private: [u8; 0],
}

/// Opaque prepared statement handle
#[repr(C)]
pub struct sqlite3_stmt {
    _private: [u8; 0],
}

/// Opaque BLOB handle
#[repr(C)]
pub struct sqlite3_blob {
    _private: [u8; 0],
}

/// Opaque backup handle
#[repr(C)]
pub struct sqlite3_backup {
    _private: [u8; 0],
}

/// Opaque value handle
#[repr(C)]
pub struct sqlite3_value {
    _private: [u8; 0],
}

/// Opaque context handle
#[repr(C)]
pub struct sqlite3_context {
    _private: [u8; 0],
}

// Result codes
pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ERROR: c_int = 1;
pub const SQLITE_INTERNAL: c_int = 2;
pub const SQLITE_PERM: c_int = 3;
pub const SQLITE_ABORT: c_int = 4;
pub const SQLITE_BUSY: c_int = 5;
pub const SQLITE_LOCKED: c_int = 6;
pub const SQLITE_NOMEM: c_int = 7;
pub const SQLITE_READONLY: c_int = 8;
pub const SQLITE_INTERRUPT: c_int = 9;
pub const SQLITE_IOERR: c_int = 10;
pub const SQLITE_CORRUPT: c_int = 11;
pub const SQLITE_NOTFOUND: c_int = 12;
pub const SQLITE_FULL: c_int = 13;
pub const SQLITE_CANTOPEN: c_int = 14;
pub const SQLITE_PROTOCOL: c_int = 15;
pub const SQLITE_EMPTY: c_int = 16;
pub const SQLITE_SCHEMA: c_int = 17;
pub const SQLITE_TOOBIG: c_int = 18;
pub const SQLITE_CONSTRAINT: c_int = 19;
pub const SQLITE_MISMATCH: c_int = 20;
pub const SQLITE_MISUSE: c_int = 21;
pub const SQLITE_NOLFS: c_int = 22;
pub const SQLITE_AUTH: c_int = 23;
pub const SQLITE_FORMAT: c_int = 24;
pub const SQLITE_RANGE: c_int = 25;
pub const SQLITE_NOTADB: c_int = 26;
pub const SQLITE_NOTICE: c_int = 27;
pub const SQLITE_WARNING: c_int = 28;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;

// Extended result codes
pub const SQLITE_ERROR_MISSING_COLLSEQ: c_int = SQLITE_ERROR | (1 << 8);
pub const SQLITE_ERROR_RETRY: c_int = SQLITE_ERROR | (2 << 8);
pub const SQLITE_IOERR_READ: c_int = SQLITE_IOERR | (1 << 8);
pub const SQLITE_IOERR_WRITE: c_int = SQLITE_IOERR | (3 << 8);
pub const SQLITE_BUSY_RECOVERY: c_int = SQLITE_BUSY | (1 << 8);
pub const SQLITE_BUSY_SNAPSHOT: c_int = SQLITE_BUSY | (2 << 8);
pub const SQLITE_BUSY_TIMEOUT: c_int = SQLITE_BUSY | (3 << 8);
pub const SQLITE_LOCKED_SHAREDCACHE: c_int = SQLITE_LOCKED | (1 << 8);
pub const SQLITE_CONSTRAINT_CHECK: c_int = SQLITE_CONSTRAINT | (1 << 8);
pub const SQLITE_CONSTRAINT_FOREIGNKEY: c_int = SQLITE_CONSTRAINT | (3 << 8);
pub const SQLITE_CONSTRAINT_NOTNULL: c_int = SQLITE_CONSTRAINT | (5 << 8);
pub const SQLITE_CONSTRAINT_PRIMARYKEY: c_int = SQLITE_CONSTRAINT | (6 << 8);
pub const SQLITE_CONSTRAINT_UNIQUE: c_int = SQLITE_CONSTRAINT | (8 << 8);

// Open flags
pub const SQLITE_OPEN_READONLY: c_int = 0x00000001;
pub const SQLITE_OPEN_READWRITE: c_int = 0x00000002;
pub const SQLITE_OPEN_CREATE: c_int = 0x00000004;
pub const SQLITE_OPEN_DELETEONCLOSE: c_int = 0x00000008;
pub const SQLITE_OPEN_EXCLUSIVE: c_int = 0x00000010;
pub const SQLITE_OPEN_URI: c_int = 0x00000040;
pub const SQLITE_OPEN_MEMORY: c_int = 0x00000080;
pub const SQLITE_OPEN_NOMUTEX: c_int = 0x00008000;
pub const SQLITE_OPEN_FULLMUTEX: c_int = 0x00010000;
pub const SQLITE_OPEN_SHAREDCACHE: c_int = 0x00020000;
pub const SQLITE_OPEN_PRIVATECACHE: c_int = 0x00040000;
pub const SQLITE_OPEN_NOFOLLOW: c_int = 0x01000000;
pub const SQLITE_OPEN_EXRESCODE: c_int = 0x02000000;

// Prepare flags
pub const SQLITE_PREPARE_PERSISTENT: c_int = 0x01;
pub const SQLITE_PREPARE_NORMALIZE: c_int = 0x02;
pub const SQLITE_PREPARE_NO_VTAB: c_int = 0x04;

// Fundamental data types
pub const SQLITE_INTEGER: c_int = 1;
pub const SQLITE_FLOAT: c_int = 2;
pub const SQLITE_TEXT: c_int = 3;
pub const SQLITE_BLOB: c_int = 4;
pub const SQLITE_NULL: c_int = 5;

// Text encoding
pub const SQLITE_UTF8: c_int = 1;
pub const SQLITE_UTF16LE: c_int = 2;
pub const SQLITE_UTF16BE: c_int = 3;
pub const SQLITE_UTF16: c_int = 4;

// Destructor type for memory management
pub type sqlite3_destructor_type = Option<unsafe extern "C" fn(*mut c_void)>;

/// SQLITE_STATIC means the content will not change and need not be freed
pub const SQLITE_STATIC: sqlite3_destructor_type = None;

/// SQLITE_TRANSIENT - get the function pointer at runtime to avoid UB
/// SQLite interprets -1 as meaning "make a copy"
#[inline]
pub fn sqlite_transient() -> sqlite3_destructor_type {
    // This is safe because SQLite specifically documents -1 as SQLITE_TRANSIENT
    unsafe { std::mem::transmute::<isize, sqlite3_destructor_type>(-1isize) }
}

// Callback type for exec
pub type sqlite3_callback =
    Option<unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int>;

#[link(name = "sqlite3")]
extern "C" {
    // Version functions
    pub fn sqlite3_libversion() -> *const c_char;
    pub fn sqlite3_sourceid() -> *const c_char;
    pub fn sqlite3_libversion_number() -> c_int;
    pub fn sqlite3_threadsafe() -> c_int;

    // Database connection
    pub fn sqlite3_open(filename: *const c_char, ppDb: *mut *mut sqlite3) -> c_int;

    pub fn sqlite3_open_v2(
        filename: *const c_char,
        ppDb: *mut *mut sqlite3,
        flags: c_int,
        zVfs: *const c_char,
    ) -> c_int;

    pub fn sqlite3_close(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_close_v2(db: *mut sqlite3) -> c_int;

    // Error handling
    pub fn sqlite3_errcode(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_extended_errcode(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    pub fn sqlite3_errstr(rc: c_int) -> *const c_char;

    // Execute SQL
    pub fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: sqlite3_callback,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;

    // Prepared statements
    pub fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        zSql: *const c_char,
        nByte: c_int,
        ppStmt: *mut *mut sqlite3_stmt,
        pzTail: *mut *const c_char,
    ) -> c_int;

    pub fn sqlite3_prepare_v3(
        db: *mut sqlite3,
        zSql: *const c_char,
        nByte: c_int,
        prepFlags: c_int,
        ppStmt: *mut *mut sqlite3_stmt,
        pzTail: *mut *const c_char,
    ) -> c_int;

    pub fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;

    // Column access
    pub fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_column_name(stmt: *mut sqlite3_stmt, N: c_int) -> *const c_char;
    pub fn sqlite3_column_type(stmt: *mut sqlite3_stmt, iCol: c_int) -> c_int;

    pub fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, iCol: c_int) -> *const c_void;
    pub fn sqlite3_column_double(stmt: *mut sqlite3_stmt, iCol: c_int) -> c_double;
    pub fn sqlite3_column_int(stmt: *mut sqlite3_stmt, iCol: c_int) -> c_int;
    pub fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, iCol: c_int) -> sqlite3_int64;
    pub fn sqlite3_column_text(stmt: *mut sqlite3_stmt, iCol: c_int) -> *const c_uchar;
    pub fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, iCol: c_int) -> c_int;

    // Parameter binding
    pub fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_bind_parameter_index(stmt: *mut sqlite3_stmt, zName: *const c_char) -> c_int;
    pub fn sqlite3_bind_parameter_name(stmt: *mut sqlite3_stmt, i: c_int) -> *const c_char;

    pub fn sqlite3_bind_blob(
        stmt: *mut sqlite3_stmt,
        i: c_int,
        value: *const c_void,
        n: c_int,
        destructor: sqlite3_destructor_type,
    ) -> c_int;

    pub fn sqlite3_bind_blob64(
        stmt: *mut sqlite3_stmt,
        i: c_int,
        value: *const c_void,
        n: sqlite3_uint64,
        destructor: sqlite3_destructor_type,
    ) -> c_int;

    pub fn sqlite3_bind_double(stmt: *mut sqlite3_stmt, i: c_int, value: c_double) -> c_int;
    pub fn sqlite3_bind_int(stmt: *mut sqlite3_stmt, i: c_int, value: c_int) -> c_int;
    pub fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, i: c_int, value: sqlite3_int64) -> c_int;
    pub fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, i: c_int) -> c_int;

    pub fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        i: c_int,
        value: *const c_char,
        n: c_int,
        destructor: sqlite3_destructor_type,
    ) -> c_int;

    pub fn sqlite3_bind_text64(
        stmt: *mut sqlite3_stmt,
        i: c_int,
        value: *const c_char,
        n: sqlite3_uint64,
        destructor: sqlite3_destructor_type,
        encoding: c_uchar,
    ) -> c_int;

    pub fn sqlite3_clear_bindings(stmt: *mut sqlite3_stmt) -> c_int;

    // Transaction control
    pub fn sqlite3_get_autocommit(db: *mut sqlite3) -> c_int;

    // Row changes
    pub fn sqlite3_changes(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_changes64(db: *mut sqlite3) -> sqlite3_int64;
    pub fn sqlite3_total_changes(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_total_changes64(db: *mut sqlite3) -> sqlite3_int64;
    pub fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> sqlite3_int64;

    // Memory management
    pub fn sqlite3_free(ptr: *mut c_void);
    pub fn sqlite3_malloc(n: c_int) -> *mut c_void;
    pub fn sqlite3_malloc64(n: sqlite3_uint64) -> *mut c_void;
    pub fn sqlite3_realloc(ptr: *mut c_void, n: c_int) -> *mut c_void;
    pub fn sqlite3_realloc64(ptr: *mut c_void, n: sqlite3_uint64) -> *mut c_void;
    pub fn sqlite3_memory_used() -> sqlite3_int64;
    pub fn sqlite3_memory_highwater(resetFlag: c_int) -> sqlite3_int64;

    // Configuration
    pub fn sqlite3_busy_timeout(db: *mut sqlite3, ms: c_int) -> c_int;
    pub fn sqlite3_extended_result_codes(db: *mut sqlite3, onoff: c_int) -> c_int;

    // BLOB I/O
    pub fn sqlite3_blob_open(
        db: *mut sqlite3,
        zDb: *const c_char,
        zTable: *const c_char,
        zColumn: *const c_char,
        iRow: sqlite3_int64,
        flags: c_int,
        ppBlob: *mut *mut sqlite3_blob,
    ) -> c_int;
    pub fn sqlite3_blob_close(blob: *mut sqlite3_blob) -> c_int;
    pub fn sqlite3_blob_bytes(blob: *mut sqlite3_blob) -> c_int;
    pub fn sqlite3_blob_read(blob: *mut sqlite3_blob, Z: *mut c_void, N: c_int, iOffset: c_int)
        -> c_int;
    pub fn sqlite3_blob_write(
        blob: *mut sqlite3_blob,
        z: *const c_void,
        n: c_int,
        iOffset: c_int,
    ) -> c_int;
    pub fn sqlite3_blob_reopen(blob: *mut sqlite3_blob, row: sqlite3_int64) -> c_int;

    // Backup API
    pub fn sqlite3_backup_init(
        pDest: *mut sqlite3,
        zDestName: *const c_char,
        pSource: *mut sqlite3,
        zSourceName: *const c_char,
    ) -> *mut sqlite3_backup;
    pub fn sqlite3_backup_step(p: *mut sqlite3_backup, nPage: c_int) -> c_int;
    pub fn sqlite3_backup_finish(p: *mut sqlite3_backup) -> c_int;
    pub fn sqlite3_backup_remaining(p: *mut sqlite3_backup) -> c_int;
    pub fn sqlite3_backup_pagecount(p: *mut sqlite3_backup) -> c_int;

    // Interrupt
    pub fn sqlite3_interrupt(db: *mut sqlite3);
    pub fn sqlite3_is_interrupted(db: *mut sqlite3) -> c_int;

    // Misc
    pub fn sqlite3_sql(stmt: *mut sqlite3_stmt) -> *const c_char;
    pub fn sqlite3_expanded_sql(stmt: *mut sqlite3_stmt) -> *mut c_char;
    pub fn sqlite3_stmt_readonly(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_stmt_busy(stmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_db_handle(stmt: *mut sqlite3_stmt) -> *mut sqlite3;
    pub fn sqlite3_db_filename(db: *mut sqlite3, zDbName: *const c_char) -> *const c_char;
    pub fn sqlite3_db_readonly(db: *mut sqlite3, zDbName: *const c_char) -> c_int;

    // Initialize/Shutdown
    pub fn sqlite3_initialize() -> c_int;
    pub fn sqlite3_shutdown() -> c_int;
}

/// Convert a SQLite result code to a Result type
#[inline]
pub fn check_result(rc: c_int) -> Result<(), c_int> {
    if rc == SQLITE_OK {
        Ok(())
    } else {
        Err(rc)
    }
}

/// Check if a step result indicates more rows
#[inline]
pub fn is_row(rc: c_int) -> bool {
    rc == SQLITE_ROW
}

/// Check if a step result indicates completion
#[inline]
pub fn is_done(rc: c_int) -> bool {
    rc == SQLITE_DONE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_version() {
        unsafe {
            let version = sqlite3_libversion_number();
            assert!(version >= 3000000, "SQLite version should be at least 3.0.0");
        }
    }

    #[test]
    fn test_threadsafe() {
        unsafe {
            let ts = sqlite3_threadsafe();
            // Should be compiled with threading support
            assert!(ts > 0, "SQLite should be thread-safe");
        }
    }
}
