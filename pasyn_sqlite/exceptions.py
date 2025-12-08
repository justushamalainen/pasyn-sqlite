"""Custom exceptions for pasyn-sqlite."""

import sqlite3


# Re-export sqlite3 exceptions for convenience
DatabaseError = sqlite3.DatabaseError
DataError = sqlite3.DataError
Error = sqlite3.Error
IntegrityError = sqlite3.IntegrityError
InterfaceError = sqlite3.InterfaceError
InternalError = sqlite3.InternalError
NotSupportedError = sqlite3.NotSupportedError
OperationalError = sqlite3.OperationalError
ProgrammingError = sqlite3.ProgrammingError
Warning = sqlite3.Warning


class PoolError(Exception):
    """Base exception for connection pool errors."""

    pass


class PoolClosedError(PoolError):
    """Raised when attempting to use a closed pool."""

    pass


class ConnectionClosedError(PoolError):
    """Raised when attempting to use a closed connection."""

    pass


class TransactionError(PoolError):
    """Base exception for transaction errors."""

    pass


class TransactionCommandError(TransactionError):
    """Raised when transaction commands are used on Pool (use bound_connection instead)."""

    pass
