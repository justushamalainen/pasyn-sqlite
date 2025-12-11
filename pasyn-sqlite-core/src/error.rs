//! Error types for pasyn-sqlite-core
//!
//! This module defines all error types used by the library.

use std::ffi::CStr;
use std::fmt;
use std::os::raw::c_int;

use crate::ffi;

/// Result type for SQLite operations
pub type Result<T> = std::result::Result<T, Error>;

/// SQLite error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Generic error
    Error,
    /// Internal logic error in SQLite
    Internal,
    /// Access permission denied
    Permission,
    /// Callback routine requested an abort
    Abort,
    /// The database file is locked
    Busy,
    /// A table in the database is locked
    Locked,
    /// A malloc() failed
    NoMemory,
    /// Attempt to write a readonly database
    ReadOnly,
    /// Operation terminated by sqlite3_interrupt()
    Interrupt,
    /// Some kind of disk I/O error occurred
    IoError,
    /// The database disk image is malformed
    Corrupt,
    /// Unknown opcode in sqlite3_file_control()
    NotFound,
    /// Insertion failed because database is full
    Full,
    /// Unable to open the database file
    CantOpen,
    /// Database lock protocol error
    Protocol,
    /// The database schema changed
    Schema,
    /// String or BLOB exceeds size limit
    TooBig,
    /// Abort due to constraint violation
    Constraint,
    /// Data type mismatch
    Mismatch,
    /// Library used incorrectly
    Misuse,
    /// Uses OS features not supported on host
    NoLfs,
    /// Authorization denied
    Auth,
    /// 2nd parameter to sqlite3_bind out of range
    Range,
    /// File opened that is not a database file
    NotADatabase,
    /// Unknown error code
    Unknown(i32),
}

impl ErrorCode {
    /// Create an ErrorCode from a raw SQLite result code
    pub fn from_raw(code: c_int) -> Self {
        // Mask to get the primary result code (lower 8 bits)
        let primary = code & 0xFF;
        match primary {
            ffi::SQLITE_ERROR => ErrorCode::Error,
            ffi::SQLITE_INTERNAL => ErrorCode::Internal,
            ffi::SQLITE_PERM => ErrorCode::Permission,
            ffi::SQLITE_ABORT => ErrorCode::Abort,
            ffi::SQLITE_BUSY => ErrorCode::Busy,
            ffi::SQLITE_LOCKED => ErrorCode::Locked,
            ffi::SQLITE_NOMEM => ErrorCode::NoMemory,
            ffi::SQLITE_READONLY => ErrorCode::ReadOnly,
            ffi::SQLITE_INTERRUPT => ErrorCode::Interrupt,
            ffi::SQLITE_IOERR => ErrorCode::IoError,
            ffi::SQLITE_CORRUPT => ErrorCode::Corrupt,
            ffi::SQLITE_NOTFOUND => ErrorCode::NotFound,
            ffi::SQLITE_FULL => ErrorCode::Full,
            ffi::SQLITE_CANTOPEN => ErrorCode::CantOpen,
            ffi::SQLITE_PROTOCOL => ErrorCode::Protocol,
            ffi::SQLITE_SCHEMA => ErrorCode::Schema,
            ffi::SQLITE_TOOBIG => ErrorCode::TooBig,
            ffi::SQLITE_CONSTRAINT => ErrorCode::Constraint,
            ffi::SQLITE_MISMATCH => ErrorCode::Mismatch,
            ffi::SQLITE_MISUSE => ErrorCode::Misuse,
            ffi::SQLITE_NOLFS => ErrorCode::NoLfs,
            ffi::SQLITE_AUTH => ErrorCode::Auth,
            ffi::SQLITE_RANGE => ErrorCode::Range,
            ffi::SQLITE_NOTADB => ErrorCode::NotADatabase,
            _ => ErrorCode::Unknown(primary),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::Error => write!(f, "SQL error or missing database"),
            ErrorCode::Internal => write!(f, "Internal logic error in SQLite"),
            ErrorCode::Permission => write!(f, "Access permission denied"),
            ErrorCode::Abort => write!(f, "Callback routine requested an abort"),
            ErrorCode::Busy => write!(f, "The database file is locked"),
            ErrorCode::Locked => write!(f, "A table in the database is locked"),
            ErrorCode::NoMemory => write!(f, "A malloc() failed"),
            ErrorCode::ReadOnly => write!(f, "Attempt to write a readonly database"),
            ErrorCode::Interrupt => write!(f, "Operation terminated by interrupt"),
            ErrorCode::IoError => write!(f, "Some kind of disk I/O error occurred"),
            ErrorCode::Corrupt => write!(f, "The database disk image is malformed"),
            ErrorCode::NotFound => write!(f, "Unknown opcode"),
            ErrorCode::Full => write!(f, "Insertion failed because database is full"),
            ErrorCode::CantOpen => write!(f, "Unable to open the database file"),
            ErrorCode::Protocol => write!(f, "Database lock protocol error"),
            ErrorCode::Schema => write!(f, "The database schema changed"),
            ErrorCode::TooBig => write!(f, "String or BLOB exceeds size limit"),
            ErrorCode::Constraint => write!(f, "Abort due to constraint violation"),
            ErrorCode::Mismatch => write!(f, "Data type mismatch"),
            ErrorCode::Misuse => write!(f, "Library used incorrectly"),
            ErrorCode::NoLfs => write!(f, "Uses OS features not supported on host"),
            ErrorCode::Auth => write!(f, "Authorization denied"),
            ErrorCode::Range => write!(f, "Bind parameter out of range"),
            ErrorCode::NotADatabase => write!(f, "File opened that is not a database file"),
            ErrorCode::Unknown(code) => write!(f, "Unknown error code: {}", code),
        }
    }
}

/// Main error type for SQLite operations
#[derive(Debug)]
pub struct Error {
    /// The error code
    pub code: ErrorCode,
    /// The extended error code (if available)
    pub extended_code: Option<i32>,
    /// Error message
    pub message: String,
}

impl Error {
    /// Create a new error from a result code
    pub fn new(code: c_int) -> Self {
        let message = unsafe {
            let ptr = ffi::sqlite3_errstr(code);
            if ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };

        Error {
            code: ErrorCode::from_raw(code),
            extended_code: Some(code),
            message,
        }
    }

    /// Create an error from a database handle
    pub unsafe fn from_db(db: *mut ffi::sqlite3) -> Self {
        let code = ffi::sqlite3_errcode(db);
        let extended_code = ffi::sqlite3_extended_errcode(db);
        let message = {
            let ptr = ffi::sqlite3_errmsg(db);
            if ptr.is_null() {
                "Unknown error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };

        Error {
            code: ErrorCode::from_raw(code),
            extended_code: Some(extended_code),
            message,
        }
    }

    /// Create an error with a custom message
    pub fn with_message(code: ErrorCode, message: impl Into<String>) -> Self {
        Error {
            code,
            extended_code: None,
            message: message.into(),
        }
    }

    /// Check if this is a busy error
    pub fn is_busy(&self) -> bool {
        matches!(self.code, ErrorCode::Busy)
    }

    /// Check if this is a locked error
    pub fn is_locked(&self) -> bool {
        matches!(self.code, ErrorCode::Locked)
    }

    /// Check if this is a constraint error
    pub fn is_constraint(&self) -> bool {
        matches!(self.code, ErrorCode::Constraint)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ext_code) = self.extended_code {
            write!(f, "{} (code {}): {}", self.code, ext_code, self.message)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}

impl std::error::Error for Error {}

impl From<std::ffi::NulError> for Error {
    fn from(e: std::ffi::NulError) -> Self {
        Error::with_message(ErrorCode::Error, format!("Invalid string: {}", e))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::with_message(ErrorCode::Error, format!("Invalid UTF-8: {}", e))
    }
}

/// Convenience macro for checking SQLite result codes
#[macro_export]
macro_rules! check {
    ($db:expr, $rc:expr) => {{
        let rc = $rc;
        if rc != $crate::ffi::SQLITE_OK {
            return Err(unsafe { $crate::Error::from_db($db) });
        }
    }};
}

/// Convenience macro for checking SQLite result codes without db handle
#[macro_export]
macro_rules! check_rc {
    ($rc:expr) => {{
        let rc = $rc;
        if rc != $crate::ffi::SQLITE_OK {
            return Err($crate::Error::new(rc));
        }
    }};
}
