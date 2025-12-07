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

# Writer Server and Client

class WriterServerHandle:
    """Handle to a running writer server."""

    @property
    def socket_path(self) -> str:
        """Get the socket path."""
        ...

    def shutdown(self) -> None:
        """Signal the server to shutdown."""
        ...

    def join(self) -> None:
        """Wait for the server to stop."""
        ...

    def stop(self) -> None:
        """Shutdown and wait for the server to stop."""
        ...

class WriterClient:
    """Client for sending write operations to the writer server."""

    def __init__(self, socket_path: str) -> None:
        """Connect to a writer server."""
        ...

    def execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement."""
        ...

    def execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement and return the last insert rowid."""
        ...

    def executescript(self, sql: str) -> None:
        """Execute multiple SQL statements (batch)."""
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

    def ping(self) -> None:
        """Ping the server."""
        ...

    def shutdown_server(self) -> None:
        """Shutdown the server."""
        ...

class HybridConnection:
    """
    A hybrid connection that reads locally and writes via the writer server.

    This is the recommended way to use the library for concurrent access:
    - Read operations are performed directly on a local read-only connection
    - Write operations are sent to the writer server via Unix socket
    """

    def __init__(self, database_path: str, socket_path: str) -> None:
        """
        Create a new hybrid connection.

        Args:
            database_path: Path to the SQLite database
            socket_path: Path to the writer server Unix socket
        """
        ...

    def execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation via the writer server."""
        ...

    def execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation and return the last insert rowid."""
        ...

    def executescript(self, sql: str) -> None:
        """Execute multiple SQL statements via the writer server."""
        ...

    def query_fetchall(self, sql: str, params: SqliteParams = None) -> List[SqliteRow]:
        """Query data locally (read-only) and return all rows."""
        ...

    def query_fetchone(self, sql: str, params: SqliteParams = None) -> Optional[SqliteRow]:
        """Query data locally and return the first row."""
        ...

    def begin(self) -> None:
        """Begin a transaction (on the writer server)."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    def ping(self) -> None:
        """Ping the writer server."""
        ...

def start_writer_server(
    database_path: str,
    socket_path: Optional[str] = None
) -> WriterServerHandle:
    """
    Start a writer server that handles all write operations via Unix socket.

    Args:
        database_path: Path to the SQLite database
        socket_path: Path for the Unix socket (optional, uses default if not specified)

    Returns:
        Handle to the running server
    """
    ...

def default_socket_path(database_path: str) -> str:
    """Get the default socket path for a database."""
    ...

def hybrid_connect(database_path: str, socket_path: str) -> HybridConnection:
    """Connect to a hybrid connection (convenience function)."""
    ...

# =============================================================================
# Async Classes (true async, no thread pool)
# =============================================================================

class AsyncWriterClient:
    """
    True async client for sending write operations to the writer server.

    Uses asyncio's native socket operations - no thread pool, pure async I/O.
    """

    def __init__(self, socket_path: str) -> None:
        """Initialize the async client (call connect() to actually connect)."""
        ...

    async def connect(self) -> None:
        """Connect to the writer server asynchronously."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    async def write_execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement asynchronously."""
        ...

    async def write_execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement and return the last insert rowid."""
        ...

    async def write_executescript(self, sql: str) -> None:
        """Execute multiple SQL statements asynchronously."""
        ...

    async def write_begin(self) -> None:
        """Begin a transaction asynchronously."""
        ...

    async def write_commit(self) -> None:
        """Commit the current transaction asynchronously."""
        ...

    async def write_rollback(self) -> None:
        """Rollback the current transaction asynchronously."""
        ...

    async def write_ping(self) -> None:
        """Ping the server asynchronously."""
        ...

    async def write_shutdown_server(self) -> None:
        """Shutdown the server asynchronously."""
        ...


class AsyncHybridConnection:
    """
    True async hybrid connection - reads locally, writes via async socket.

    - Read operations are synchronous (fast, local SQLite, no I/O wait)
    - Write operations use true async I/O (non-blocking socket, event loop driven)
    """

    def __init__(self, database_path: str, socket_path: str) -> None:
        """Initialize the async hybrid connection (call connect() to connect)."""
        ...

    async def connect(self) -> None:
        """Connect to the database (read) and writer server (write)."""
        ...

    def close(self) -> None:
        """Close all connections."""
        ...

    # Async write methods
    async def write_execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation via the writer server (async)."""
        ...

    async def write_execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation and return the last insert rowid (async)."""
        ...

    async def write_executescript(self, sql: str) -> None:
        """Execute multiple SQL statements via the writer server (async)."""
        ...

    async def write_begin(self) -> None:
        """Begin a transaction on the writer server (async)."""
        ...

    async def write_commit(self) -> None:
        """Commit the current transaction (async)."""
        ...

    async def write_rollback(self) -> None:
        """Rollback the current transaction (async)."""
        ...

    async def write_ping(self) -> None:
        """Ping the writer server (async)."""
        ...

    # Sync read methods (local, fast)
    def query_fetchall(self, sql: str, params: SqliteParams = None) -> List[SqliteRow]:
        """Query data locally (read-only) and return all rows."""
        ...

    def query_fetchone(self, sql: str, params: SqliteParams = None) -> Optional[SqliteRow]:
        """Query data locally and return the first row."""
        ...


async def async_hybrid_connect(database_path: str, socket_path: str) -> AsyncHybridConnection:
    """Create and connect an async hybrid connection."""
    ...


async def async_writer_client(socket_path: str) -> AsyncWriterClient:
    """Create and connect an async writer client."""
    ...


# =============================================================================
# Protocol Serialization (for custom async implementations)
# =============================================================================

def serialize_execute_request(sql: str, params: SqliteParams = None) -> bytes:
    """Serialize an execute request to bytes."""
    ...

def serialize_execute_returning_rowid_request(sql: str, params: SqliteParams = None) -> bytes:
    """Serialize an execute_returning_rowid request to bytes."""
    ...

def serialize_executescript_request(sql: str) -> bytes:
    """Serialize an executescript request to bytes."""
    ...

def serialize_begin_request() -> bytes:
    """Serialize a begin transaction request to bytes."""
    ...

def serialize_commit_request() -> bytes:
    """Serialize a commit request to bytes."""
    ...

def serialize_rollback_request() -> bytes:
    """Serialize a rollback request to bytes."""
    ...

def serialize_ping_request() -> bytes:
    """Serialize a ping request to bytes."""
    ...

def serialize_shutdown_request() -> bytes:
    """Serialize a shutdown request to bytes."""
    ...

def parse_response(data: bytes) -> Tuple[bool, int, int, Optional[str]]:
    """
    Parse response from bytes.

    Returns:
        Tuple of (success, rows_affected, last_insert_rowid, error_message)
    """
    ...
