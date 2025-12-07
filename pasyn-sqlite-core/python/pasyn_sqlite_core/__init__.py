"""
pasyn-sqlite-core - High-performance SQLite bindings for Python via Rust

This module provides a fast SQLite interface implemented in Rust with Python bindings.

Example usage:

    import pasyn_sqlite_core as sqlite

    # Connect to database
    conn = sqlite.connect(":memory:")

    # Create table
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

    # Insert data
    conn.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])

    # Query data
    rows = conn.execute_fetchall("SELECT * FROM users")
    print(rows)  # [(1, 'Alice')]

    # With context manager
    with sqlite.connect("mydb.sqlite") as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS data (value TEXT)")
        conn.execute("INSERT INTO data VALUES (?)", ["hello"])
        conn.commit()

Writer Server Example (sync):

    import pasyn_sqlite_core as sqlite

    # Start writer server
    server = sqlite.start_writer_server("mydb.sqlite")
    print(f"Server running on {server.socket_path}")

    # Use hybrid connection for concurrent access
    conn = sqlite.hybrid_connect("mydb.sqlite", server.socket_path)

    # Write via server (sync)
    conn.execute("INSERT INTO users (name) VALUES (?)", ["Bob"])

    # Read locally (sync)
    rows = conn.query_fetchall("SELECT * FROM users")

    # Stop server when done
    server.stop()

Async Writer Example (true async, no thread pool):

    import asyncio
    import pasyn_sqlite_core as sqlite

    async def main():
        # Start writer server
        server = sqlite.start_writer_server("mydb.sqlite")

        # Use async hybrid connection (true async I/O)
        conn = await sqlite.async_hybrid_connect("mydb.sqlite", server.socket_path)

        # Async writes via server (non-blocking socket I/O)
        await conn.write_execute("INSERT INTO users (name) VALUES (?)", ["Bob"])
        await conn.write_executescript("INSERT INTO logs VALUES (1); INSERT INTO logs VALUES (2)")

        # Sync reads (fast, local) - no await needed
        rows = conn.query_fetchall("SELECT * FROM users")

        # Close async connection
        conn.close()

        # Stop server
        server.stop()

    asyncio.run(main())
"""

import asyncio
import socket
import struct
from typing import Any, List, Optional, Tuple, Sequence, Union

from .pasyn_sqlite_core import (
    # Classes
    Connection,
    Cursor,
    OpenFlags,
    SqliteError,
    # Writer Server classes
    WriterServerHandle,
    WriterClient,
    HybridConnection,
    # Native async client (GIL-releasing awaitables)
    NativeAsyncClient,
    NativeExecuteAwaitable,
    native_async_client,
    # Persistent native async client (reuses connection)
    PersistentNativeAsyncClient,
    PersistentNativeExecuteAwaitable,
    persistent_native_async_client,
    # Multiplexed client (thread-safe with automatic batching)
    MultiplexedClient,
    multiplexed_client,
    # Connection functions
    connect,
    hybrid_connect,
    # Server functions
    start_writer_server,
    default_socket_path,
    # Protocol serialization (for async I/O)
    serialize_execute_request,
    serialize_execute_returning_rowid_request,
    serialize_executescript_request,
    serialize_begin_request,
    serialize_commit_request,
    serialize_rollback_request,
    serialize_ping_request,
    serialize_shutdown_request,
    parse_response,
    # Utility functions
    sqlite_version,
    sqlite_version_number,
    sqlite_threadsafe,
    memory_used,
    memory_highwater,
)

# Type aliases
SqliteValue = Union[None, int, float, str, bytes]
SqliteRow = Tuple[SqliteValue, ...]
SqliteParams = Optional[Union[Sequence[SqliteValue], dict]]


class NativeHybridConnection:
    """
    Hybrid connection with native awaitable writes (GIL-releasing).

    - Read operations are synchronous (fast, local SQLite)
    - Write operations use native Rust awaitables that release the GIL

    This is similar to AsyncHybridConnection but uses native Rust awaitables
    instead of Python's asyncio socket operations.

    Uses a persistent socket connection for better performance.
    """

    def __init__(self, database_path: str, socket_path: str):
        """
        Initialize the native hybrid connection.

        Args:
            database_path: Path to the SQLite database
            socket_path: Path to the writer server Unix socket
        """
        # Open read-only connection for local reads
        self._read_conn = Connection(database_path, OpenFlags.readonly())
        # Create persistent native async client for writes (reuses connection)
        self._write_client = PersistentNativeAsyncClient(socket_path)
        self._socket_path = socket_path

    def close(self) -> None:
        """Close all connections."""
        if self._write_client:
            self._write_client.close()
            self._write_client = None
        if self._read_conn:
            self._read_conn.close()
            self._read_conn = None

    # =========================================================================
    # Async write methods (native awaitables with GIL release)
    # =========================================================================

    async def write_execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation (async, GIL-releasing)."""
        result = await self._write_client.execute(sql, params)
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")
        return rows_affected

    async def write_execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation and return the last insert rowid (async)."""
        result = await self._write_client.execute_returning_rowid(sql, params)
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")
        return last_rowid

    async def write_executescript(self, sql: str) -> None:
        """Execute multiple SQL statements (async)."""
        result = await self._write_client.executescript(sql)
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")

    async def write_begin(self) -> None:
        """Begin a transaction (async)."""
        result = await self._write_client.begin()
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")

    async def write_commit(self) -> None:
        """Commit the current transaction (async)."""
        result = await self._write_client.commit()
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")

    async def write_rollback(self) -> None:
        """Rollback the current transaction (async)."""
        result = await self._write_client.rollback()
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")

    async def write_ping(self) -> None:
        """Ping the writer server (async)."""
        result = await self._write_client.ping()
        success, rows_affected, last_rowid, error_msg = result
        if not success:
            raise SqliteError(error_msg or "Unknown error")

    # =========================================================================
    # Sync read methods (local SQLite, fast)
    # =========================================================================

    def query_fetchall(self, sql: str, params: SqliteParams = None) -> List[SqliteRow]:
        """Query data locally (read-only) and return all rows."""
        return self._read_conn.execute_fetchall(sql, params)

    def query_fetchone(self, sql: str, params: SqliteParams = None) -> Optional[SqliteRow]:
        """Query data locally and return the first row."""
        return self._read_conn.execute_fetchone(sql, params)


def native_hybrid_connect(database_path: str, socket_path: str) -> NativeHybridConnection:
    """
    Create a native hybrid connection.

    Args:
        database_path: Path to the SQLite database
        socket_path: Path to the writer server Unix socket

    Returns:
        NativeHybridConnection instance
    """
    return NativeHybridConnection(database_path, socket_path)


class AsyncWriterClient:
    """
    True async client for sending write operations to the writer server.

    Uses asyncio's native socket operations - no thread pool, pure async I/O.
    The event loop handles I/O multiplexing efficiently.
    """

    def __init__(self, socket_path: str):
        """
        Initialize the async client (call connect() to actually connect).

        Args:
            socket_path: Path to the writer server Unix socket
        """
        self._socket_path = socket_path
        self._sock: Optional[socket.socket] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self) -> None:
        """Connect to the writer server asynchronously."""
        self._loop = asyncio.get_running_loop()
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.setblocking(False)
        await self._loop.sock_connect(self._sock, self._socket_path)

    def close(self) -> None:
        """Close the connection."""
        if self._sock:
            self._sock.close()
            self._sock = None

    async def _send_request(self, request_bytes: bytes) -> Tuple[bool, int, int, Optional[str]]:
        """Send a request and receive the response."""
        if not self._sock or not self._loop:
            raise RuntimeError("Not connected. Call connect() first.")

        # Send the request (already includes length prefix)
        await self._loop.sock_sendall(self._sock, request_bytes)

        # Read response length (4 bytes)
        length_bytes = await self._recv_exact(4)
        response_length = struct.unpack('<I', length_bytes)[0]

        # Read response data
        response_data = await self._recv_exact(response_length)

        # Parse and return response
        return parse_response(response_data)

    async def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes from the socket."""
        if not self._sock or not self._loop:
            raise RuntimeError("Not connected")

        data = b''
        while len(data) < n:
            chunk = await self._loop.sock_recv(self._sock, n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed by server")
            data += chunk
        return data

    def _check_response(self, response: Tuple[bool, int, int, Optional[str]]) -> None:
        """Check response and raise on error."""
        success, rows_affected, last_rowid, error_msg = response
        if not success:
            raise SqliteError(error_msg or "Unknown error")

    async def write_execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement asynchronously."""
        request = serialize_execute_request(sql, params)
        response = await self._send_request(request)
        self._check_response(response)
        return response[1]  # rows_affected

    async def write_execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a SQL statement and return the last insert rowid."""
        request = serialize_execute_returning_rowid_request(sql, params)
        response = await self._send_request(request)
        self._check_response(response)
        return response[2]  # last_insert_rowid

    async def write_executescript(self, sql: str) -> None:
        """Execute multiple SQL statements asynchronously."""
        request = serialize_executescript_request(sql)
        response = await self._send_request(request)
        self._check_response(response)

    async def write_begin(self) -> None:
        """Begin a transaction asynchronously."""
        request = serialize_begin_request()
        response = await self._send_request(request)
        self._check_response(response)

    async def write_commit(self) -> None:
        """Commit the current transaction asynchronously."""
        request = serialize_commit_request()
        response = await self._send_request(request)
        self._check_response(response)

    async def write_rollback(self) -> None:
        """Rollback the current transaction asynchronously."""
        request = serialize_rollback_request()
        response = await self._send_request(request)
        self._check_response(response)

    async def write_ping(self) -> None:
        """Ping the server asynchronously."""
        request = serialize_ping_request()
        response = await self._send_request(request)
        self._check_response(response)

    async def write_shutdown_server(self) -> None:
        """Shutdown the server asynchronously."""
        request = serialize_shutdown_request()
        response = await self._send_request(request)
        self._check_response(response)


class AsyncHybridConnection:
    """
    True async hybrid connection - reads locally, writes via async socket.

    - Read operations are synchronous (fast, local SQLite, no I/O wait)
    - Write operations use true async I/O (non-blocking socket, event loop driven)

    No thread pool is used - writes are handled by asyncio's native socket support.
    """

    def __init__(self, database_path: str, socket_path: str):
        """
        Initialize the async hybrid connection (call connect() to connect socket).

        Args:
            database_path: Path to the SQLite database
            socket_path: Path to the writer server Unix socket
        """
        self._database_path = database_path
        self._socket_path = socket_path
        self._read_conn: Optional[Connection] = None
        self._write_client: Optional[AsyncWriterClient] = None

    async def connect(self) -> None:
        """Connect to the database (read) and writer server (write)."""
        # Open read-only connection for local reads
        self._read_conn = Connection(self._database_path, OpenFlags.readonly())

        # Connect to writer server for async writes
        self._write_client = AsyncWriterClient(self._socket_path)
        await self._write_client.connect()

    def close(self) -> None:
        """Close all connections."""
        if self._write_client:
            self._write_client.close()
            self._write_client = None
        if self._read_conn:
            self._read_conn.close()
            self._read_conn = None

    # =========================================================================
    # Async write methods (true async, non-blocking socket I/O)
    # =========================================================================

    async def write_execute(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation via the writer server (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        return await self._write_client.write_execute(sql, params)

    async def write_execute_returning_rowid(self, sql: str, params: SqliteParams = None) -> int:
        """Execute a write operation and return the last insert rowid (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        return await self._write_client.write_execute_returning_rowid(sql, params)

    async def write_executescript(self, sql: str) -> None:
        """Execute multiple SQL statements via the writer server (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        await self._write_client.write_executescript(sql)

    async def write_begin(self) -> None:
        """Begin a transaction on the writer server (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        await self._write_client.write_begin()

    async def write_commit(self) -> None:
        """Commit the current transaction (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        await self._write_client.write_commit()

    async def write_rollback(self) -> None:
        """Rollback the current transaction (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        await self._write_client.write_rollback()

    async def write_ping(self) -> None:
        """Ping the writer server (async)."""
        if not self._write_client:
            raise RuntimeError("Not connected. Call connect() first.")
        await self._write_client.write_ping()

    # =========================================================================
    # Sync read methods (local SQLite, fast - no async needed)
    # =========================================================================

    def query_fetchall(self, sql: str, params: SqliteParams = None) -> List[SqliteRow]:
        """
        Query data locally (read-only) and return all rows.

        This is synchronous because reads are local and fast.
        """
        if not self._read_conn:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._read_conn.execute_fetchall(sql, params)

    def query_fetchone(self, sql: str, params: SqliteParams = None) -> Optional[SqliteRow]:
        """
        Query data locally and return the first row.

        This is synchronous because reads are local and fast.
        """
        if not self._read_conn:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._read_conn.execute_fetchone(sql, params)


async def async_hybrid_connect(database_path: str, socket_path: str) -> AsyncHybridConnection:
    """
    Create and connect an async hybrid connection.

    Args:
        database_path: Path to the SQLite database
        socket_path: Path to the writer server Unix socket

    Returns:
        Connected AsyncHybridConnection instance
    """
    conn = AsyncHybridConnection(database_path, socket_path)
    await conn.connect()
    return conn


async def async_writer_client(socket_path: str) -> AsyncWriterClient:
    """
    Create and connect an async writer client.

    Args:
        socket_path: Path to the writer server Unix socket

    Returns:
        Connected AsyncWriterClient instance
    """
    client = AsyncWriterClient(socket_path)
    await client.connect()
    return client


__all__ = [
    # Classes
    "Connection",
    "Cursor",
    "OpenFlags",
    "SqliteError",
    # Writer Server classes (sync)
    "WriterServerHandle",
    "WriterClient",
    "HybridConnection",
    # Native async classes (GIL-releasing awaitables)
    "NativeAsyncClient",
    "NativeExecuteAwaitable",
    "NativeHybridConnection",
    # Persistent native async classes (reuses connection)
    "PersistentNativeAsyncClient",
    "PersistentNativeExecuteAwaitable",
    "persistent_native_async_client",
    # Multiplexed client (thread-safe with automatic batching)
    "MultiplexedClient",
    "multiplexed_client",
    # Async classes (asyncio socket I/O)
    "AsyncHybridConnection",
    "AsyncWriterClient",
    # Connection functions
    "connect",
    "hybrid_connect",
    "native_hybrid_connect",
    "native_async_client",
    "async_hybrid_connect",
    "async_writer_client",
    # Server functions
    "start_writer_server",
    "default_socket_path",
    # Protocol serialization (for custom async implementations)
    "serialize_execute_request",
    "serialize_execute_returning_rowid_request",
    "serialize_executescript_request",
    "serialize_begin_request",
    "serialize_commit_request",
    "serialize_rollback_request",
    "serialize_ping_request",
    "serialize_shutdown_request",
    "parse_response",
    # Utility functions
    "sqlite_version",
    "sqlite_version_number",
    "sqlite_threadsafe",
    "memory_used",
    "memory_highwater",
]

__version__ = "0.1.0"
