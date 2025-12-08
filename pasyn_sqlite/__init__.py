"""
pasyn-sqlite: Async SQLite with multiplexed Rust client.

Uses a multiplexed Rust client for thread-safe, batched database operations.
"""

from .exceptions import (
    ConnectionClosedError,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    PoolClosedError,
    PoolError,
    ProgrammingError,
    TransactionCommandError,
    TransactionError,
    Warning,
)

__version__ = "0.1.0"

__all__ = [
    # Exceptions
    "PoolError",
    "PoolClosedError",
    "ConnectionClosedError",
    "TransactionError",
    "TransactionCommandError",
    # Re-exported sqlite3 exceptions
    "Error",
    "Warning",
    "DatabaseError",
    "DataError",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    # Version
    "__version__",
]
