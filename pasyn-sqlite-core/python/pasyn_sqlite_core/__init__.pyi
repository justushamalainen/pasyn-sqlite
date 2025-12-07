"""Type stubs for pasyn_sqlite_core"""

from typing import Any, List, Optional, Tuple, Union, Sequence

# Type aliases
SqliteValue = Union[None, int, float, str, bytes]
SqliteRow = Tuple[SqliteValue, ...]
SqliteParams = Optional[Union[Sequence[SqliteValue], dict[str, SqliteValue]]]

class SqliteError(Exception):
    """Exception raised for SQLite errors."""
    pass

class OpenFlags:
    """Flags for opening database connections."""

    @staticmethod
    def readonly() -> OpenFlags:
        """Create read-only flags."""
        ...

    @staticmethod
    def readwrite() -> OpenFlags:
        """Create read-write flags."""
        ...

    @staticmethod
    def create() -> OpenFlags:
        """Create read-write with create flags."""
        ...

    def uri(self) -> OpenFlags:
        """Add URI flag."""
        ...

    def memory(self) -> OpenFlags:
        """Add memory flag."""
        ...

    def shared_cache(self) -> OpenFlags:
        """Add shared cache flag."""
        ...

    def __or__(self, other: OpenFlags) -> OpenFlags:
        ...

class Cursor:
    """Database cursor for iterating over query results."""

    @property
    def description(self) -> List[Tuple[str, None, None, None, None, None, None]]:
        """Get column descriptions (name, type_code, display_size, internal_size, precision, scale, null_ok)."""
        ...

    def execute(self) -> None:
        """Execute the query."""
        ...

    def fetchone(self) -> Optional[SqliteRow]:
        """Fetch the next row."""
        ...

    def fetchmany(self, size: Optional[int] = None) -> List[SqliteRow]:
        """Fetch many rows."""
        ...

    def fetchall(self) -> List[SqliteRow]:
        """Fetch all remaining rows."""
        ...

    def __iter__(self) -> Cursor:
        ...

class Connection:
    """SQLite database connection."""

    def __init__(self, path: str, flags: Optional[OpenFlags] = None) -> None:
        """Open a database connection."""
        ...

    @staticmethod
    def memory() -> Connection:
        """Open an in-memory database."""
        ...

    @staticmethod
    def shared_memory(name: str) -> Connection:
        """Open a shared in-memory database."""
        ...

    def execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement. Returns number of rows changed."""
        ...

    def executescript(self, sql: str) -> None:
        """Execute multiple SQL statements."""
        ...

    def execute_fetchall(self, sql: str, params: SqliteParams = None) -> List[SqliteRow]:
        """Execute SQL and return all rows."""
        ...

    def execute_fetchone(self, sql: str, params: SqliteParams = None) -> Optional[SqliteRow]:
        """Execute SQL and return the first row."""
        ...

    def cursor(self, sql: str, params: SqliteParams = None) -> Cursor:
        """Create a cursor for iterating over results."""
        ...

    def begin(self) -> None:
        """Begin a transaction."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    @property
    def in_transaction(self) -> bool:
        """Check if currently in a transaction."""
        ...

    @property
    def last_insert_rowid(self) -> int:
        """Get the last inserted row ID."""
        ...

    @property
    def changes(self) -> int:
        """Get the number of rows changed by the last statement."""
        ...

    @property
    def total_changes(self) -> int:
        """Get total changes since connection opened."""
        ...

    def set_busy_timeout(self, ms: int) -> None:
        """Set busy timeout in milliseconds."""
        ...

    def interrupt(self) -> None:
        """Interrupt any pending operation."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    def __enter__(self) -> Connection:
        ...

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool:
        ...

def connect(path: str, flags: Optional[OpenFlags] = None) -> Connection:
    """Connect to a database (convenience function)."""
    ...

def sqlite_version() -> str:
    """Get SQLite version string."""
    ...

def sqlite_version_number() -> int:
    """Get SQLite version number."""
    ...

def sqlite_threadsafe() -> bool:
    """Check if SQLite is thread-safe."""
    ...

def memory_used() -> int:
    """Get memory currently used by SQLite."""
    ...

def memory_highwater(reset: bool = False) -> int:
    """Get memory high-water mark."""
    ...
