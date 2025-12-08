"""Different SQLite implementations for benchmarking."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Queue
from typing import Any, Callable, Coroutine, Sequence

import pasyn_sqlite_core


class BaseSQLiteImplementation(ABC):
    """Base class for SQLite implementations."""

    @abstractmethod
    async def setup(self, db_path: str) -> None:
        """Initialize the implementation."""
        pass

    @abstractmethod
    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        """Execute SQL and return results."""
        pass

    @abstractmethod
    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        """Execute SQL with multiple parameter sets."""
        pass

    @abstractmethod
    async def commit(self) -> None:
        """Commit the current transaction."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the implementation."""
        pass

    async def begin_transaction(self) -> None:
        """Begin a transaction."""
        await self.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.execute("ROLLBACK")

    async def run_in_transaction(
        self, operations: Callable[["BaseSQLiteImplementation"], Coroutine[Any, Any, Any]]
    ) -> None:
        """Run operations within a transaction."""
        await self.begin_transaction()
        try:
            await operations(self)
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    async def __aenter__(self) -> "BaseSQLiteImplementation":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


class MainThreadSQLite(BaseSQLiteImplementation):
    """
    SQLite running in the main thread (synchronous).

    This is the baseline - all operations block the event loop.
    """

    def __init__(self) -> None:
        self._conn: sqlite3.Connection | None = None

    async def setup(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._conn is not None
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchall()

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        assert self._conn is not None
        self._conn.executemany(sql, parameters)

    async def commit(self) -> None:
        assert self._conn is not None
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


@dataclass
class DBTask:
    """Task for the single-thread DB worker."""

    func: Callable[..., Any]
    args: tuple[Any, ...]
    future: Future[Any]


class SingleThreadSQLite(BaseSQLiteImplementation):
    """
    SQLite running in a single background thread.

    All operations (reads and writes) go through one thread.
    Uses blocking Queue.get() - no polling, zero CPU waste when idle.
    """

    def __init__(self) -> None:
        self._conn: sqlite3.Connection | None = None
        self._queue: Queue[DBTask | None] = Queue()
        self._thread: threading.Thread | None = None
        self._tx_lock: asyncio.Lock | None = None  # For serializing transactions

    async def setup(self, db_path: str) -> None:
        self._db_path = db_path
        self._tx_lock = asyncio.Lock()
        self._thread = threading.Thread(
            target=self._worker_loop, daemon=True, name="single-db-thread"
        )
        self._thread.start()
        # Wait for connection to be established
        await self._submit(lambda conn: None)

    def _worker_loop(self) -> None:
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

        while True:
            # Blocks until task available - no polling!
            task = self._queue.get()
            if task is None:  # Shutdown sentinel
                break
            try:
                result = task.func(self._conn)
                task.future.set_result(result)
            except Exception as e:
                task.future.set_exception(e)

        if self._conn:
            self._conn.close()

    async def _submit(self, func: Callable[[sqlite3.Connection], Any]) -> Any:
        future: Future[Any] = Future()
        task = DBTask(func=func, args=(), future=future)
        self._queue.put(task)
        return await asyncio.wrap_future(future)

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        def _execute(conn: sqlite3.Connection) -> list[Any]:
            cursor = conn.execute(sql, parameters)
            return cursor.fetchall()

        return await self._submit(_execute)

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        def _executemany(conn: sqlite3.Connection) -> None:
            conn.executemany(sql, parameters)

        await self._submit(_executemany)

    async def commit(self) -> None:
        await self._submit(lambda conn: conn.commit())

    async def run_in_transaction(
        self, operations: Callable[["BaseSQLiteImplementation"], Coroutine[Any, Any, Any]]
    ) -> None:
        """Run operations within a transaction - serialized with lock."""
        assert self._tx_lock is not None
        async with self._tx_lock:
            await self.begin_transaction()
            try:
                await operations(self)
                await self.commit()
            except Exception:
                await self.rollback()
                raise

    async def close(self) -> None:
        self._queue.put(None)  # Send shutdown sentinel
        if self._thread:
            self._thread.join(timeout=5.0)


class MultiplexedSQLite(BaseSQLiteImplementation):
    """
    Multiplexed client implementation using Rust thread-safe client.

    This implementation:
    - Uses a single shared connection for all operations
    - Automatically batches concurrent requests
    - Lock-free request submission (minimal contention)
    - Releases GIL during I/O operations
    - Uses local SQLite connection(s) for reads (sync, fast)
    - Supports configurable number of read threads for parallel reads
    """

    def __init__(self, num_read_threads: int = 1) -> None:
        self._server: pasyn_sqlite_core.WriterServerHandle | None = None
        self._client: pasyn_sqlite_core.MultiplexedClient | None = None
        self._read_conns: list[pasyn_sqlite_core.Connection] = []
        self._db_path: str | None = None
        self._in_transaction: bool = False
        self._num_read_threads = num_read_threads
        self._read_conn_index = 0  # Round-robin index

    async def setup(self, db_path: str) -> None:
        self._db_path = db_path
        # Start writer server
        self._server = pasyn_sqlite_core.start_writer_server(db_path)
        # Create multiplexed client for writes
        self._client = pasyn_sqlite_core.MultiplexedClient(self._server.socket_path)
        # Create pool of read-only connections for reads
        self._read_conns = []
        for _ in range(self._num_read_threads):
            conn = pasyn_sqlite_core.connect(
                db_path, pasyn_sqlite_core.OpenFlags.readonly()
            )
            self._read_conns.append(conn)

    def _get_read_conn(self) -> pasyn_sqlite_core.Connection:
        """Get next read connection using round-robin."""
        conn = self._read_conns[self._read_conn_index]
        self._read_conn_index = (self._read_conn_index + 1) % len(self._read_conns)
        return conn

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._client is not None
        assert self._read_conns
        sql_lower = sql.strip().upper()
        if sql_lower.startswith(("SELECT", "PRAGMA", "EXPLAIN", "WITH")):
            # Read operation - use local connection (sync) from pool
            read_conn = self._get_read_conn()
            return read_conn.execute_fetchall(sql, list(parameters))
        else:
            # Write operation - use multiplexed client
            # Note: this is synchronous but releases GIL during I/O
            self._client.execute(sql, list(parameters))
            return []

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        assert self._client is not None
        # Use execute_many for efficient batched execution
        # This sends all parameters in a single request
        # The server wraps this in a transaction automatically for efficiency
        self._client.execute_many(sql, [list(p) for p in parameters])

    async def commit(self) -> None:
        assert self._client is not None
        # Only send commit if in an explicit transaction
        # (server auto-commits single statements, COMMIT fails if no transaction active)
        if self._in_transaction:
            self._client.commit()
            self._in_transaction = False
        # If not in transaction, commit is a no-op (already auto-committed)

    async def begin_transaction(self) -> None:
        assert self._client is not None
        self._client.begin()
        self._in_transaction = True

    async def rollback(self) -> None:
        assert self._client is not None
        if self._in_transaction:
            self._client.rollback()
            self._in_transaction = False
        # If not in transaction, rollback is a no-op

    async def run_in_transaction(
        self, operations: Callable[["BaseSQLiteImplementation"], Coroutine[Any, Any, Any]]
    ) -> None:
        """Run operations within a transaction."""
        await self.begin_transaction()
        try:
            await operations(self)
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    async def close(self) -> None:
        for conn in self._read_conns:
            conn.close()
        self._read_conns = []
        self._client = None  # No explicit close needed
        if self._server:
            self._server.stop()
            self._server = None


class MultiplexedWithReaderPool(BaseSQLiteImplementation):
    """
    Multiplexed client with Rust-side reader thread pool.

    This implementation:
    - Uses the writer server for all write operations
    - Uses a Rust-side reader pool for read operations
    - True parallel reads in Rust threads (bypasses Python GIL)
    - Each reader thread has its own SQLite connection
    """

    def __init__(self, num_read_threads: int = 3) -> None:
        self._server: pasyn_sqlite_core.WriterServerHandle | None = None
        self._client: pasyn_sqlite_core.MultiplexedClient | None = None
        self._reader_pool: pasyn_sqlite_core.ReaderPool | None = None
        self._db_path: str | None = None
        self._in_transaction: bool = False
        self._num_read_threads = num_read_threads
        self._pending_commit: bool = False  # Track if a commit is pending after executescript

    async def setup(self, db_path: str) -> None:
        self._db_path = db_path
        # Start writer server
        self._server = pasyn_sqlite_core.start_writer_server(db_path)
        # Create multiplexed client for writes
        self._client = pasyn_sqlite_core.MultiplexedClient(self._server.socket_path)
        # Create reader pool for reads
        self._reader_pool = pasyn_sqlite_core.ReaderPool(db_path, self._num_read_threads)

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._client is not None
        assert self._reader_pool is not None
        sql_lower = sql.strip().upper()
        if sql_lower.startswith(("SELECT", "PRAGMA", "EXPLAIN", "WITH")):
            # Read operation - use reader pool (releases GIL during execution)
            return self._reader_pool.query(sql, list(parameters))
        else:
            # Write operation - use multiplexed client
            self._client.execute(sql, list(parameters))
            return []

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        assert self._client is not None
        self._client.execute_many(sql, [list(p) for p in parameters])

    async def executescript(self, script: str) -> None:
        assert self._client is not None
        self._client.executescript(script)
        # executescript auto-commits, so mark that we shouldn't commit again
        self._pending_commit = True

    async def begin_transaction(self) -> None:
        assert self._client is not None
        self._in_transaction = True
        self._pending_commit = False
        self._client.begin()

    async def commit(self) -> None:
        assert self._client is not None
        # Skip commit if executescript already committed
        if self._pending_commit:
            self._pending_commit = False
            return
        if self._in_transaction:
            self._in_transaction = False
            self._client.commit()

    async def rollback(self) -> None:
        assert self._client is not None
        self._in_transaction = False
        self._pending_commit = False
        self._client.rollback()

    async def run_in_transaction(
        self, operations: Callable[["BaseSQLiteImplementation"], Coroutine[Any, Any, Any]]
    ) -> None:
        """Run operations within a transaction."""
        await self.begin_transaction()
        try:
            await operations(self)
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    async def close(self) -> None:
        if self._reader_pool:
            self._reader_pool.shutdown()
            self._reader_pool = None
        self._client = None
        if self._server:
            self._server.stop()
            self._server = None


# Factory function
def create_implementation(name: str, **kwargs: Any) -> BaseSQLiteImplementation:
    """Create an implementation by name."""
    implementations: dict[str, type[BaseSQLiteImplementation]] = {
        "main_thread": MainThreadSQLite,
        "single_thread": SingleThreadSQLite,
        "multiplexed": MultiplexedSQLite,
        "multiplexed_reader_pool": MultiplexedWithReaderPool,
    }
    if name not in implementations:
        raise ValueError(f"Unknown implementation: {name}")
    return implementations[name](**kwargs)
