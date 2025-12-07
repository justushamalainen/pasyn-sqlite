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

import pasyn_sqlite
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


class PasynPoolSQLite(BaseSQLiteImplementation):
    """
    PasynPool implementation with bound_connection for transactions.

    Uses:
    - pool.execute() for simple queries (auto-routes, auto-commits)
    - pool.bound_connection() for transactions
    """

    def __init__(self, num_readers: int = 3) -> None:
        self._num_readers = num_readers
        self._pool: pasyn_sqlite.PasynPool | None = None

    async def setup(self, db_path: str) -> None:
        self._pool = await pasyn_sqlite.create_pool(
            db_path, num_readers=self._num_readers
        )

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._pool is not None
        cursor = await self._pool.execute(sql, parameters)
        return await cursor.fetchall()

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        assert self._pool is not None
        await self._pool.executemany(sql, parameters)

    async def commit(self) -> None:
        # PasynPool auto-commits, this is a no-op for non-transaction use
        pass

    async def begin_transaction(self) -> None:
        # For PasynPool, transactions require bound_connection
        # This won't work with pool.execute directly
        raise NotImplementedError(
            "Use run_in_transaction() or get bound_connection for transactions"
        )

    async def rollback(self) -> None:
        raise NotImplementedError(
            "Use run_in_transaction() or get bound_connection for transactions"
        )

    async def run_in_transaction(
        self, operations: Callable[["BaseSQLiteImplementation"], Coroutine[Any, Any, Any]]
    ) -> None:
        """Run operations within a transaction using bound_connection."""
        assert self._pool is not None
        async with await self._pool.bound_connection() as conn:
            await conn.execute("BEGIN")
            try:
                # Create a wrapper that uses the bound connection
                wrapper = BoundConnectionWrapper(conn)
                await operations(wrapper)
                await conn.execute("COMMIT")
            except Exception:
                await conn.execute("ROLLBACK")
                raise

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None


class BoundConnectionWrapper(BaseSQLiteImplementation):
    """Wrapper around BoundConnection to match BaseSQLiteImplementation interface."""

    def __init__(self, conn: pasyn_sqlite.BoundConnection) -> None:
        self._conn = conn

    async def setup(self, db_path: str) -> None:
        pass  # Already set up

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        cursor = await self._conn.execute(sql, parameters)
        return await cursor.fetchall()

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        await self._conn.executemany(sql, parameters)

    async def commit(self) -> None:
        await self._conn.commit()

    async def close(self) -> None:
        pass  # Managed by context manager


class NativeAwaitableSQLite(BaseSQLiteImplementation):
    """
    Native awaitable implementation using Rust GIL-releasing awaitables.

    This implementation:
    - Uses native Rust awaitables for writes (releases GIL during socket I/O)
    - Uses local SQLite connection for reads (sync, fast)
    - Writes go through a writer server via Unix socket
    """

    def __init__(self) -> None:
        self._server: pasyn_sqlite_core.WriterServerHandle | None = None
        self._conn: pasyn_sqlite_core.NativeHybridConnection | None = None
        self._db_path: str | None = None
        self._in_transaction: bool = False

    async def setup(self, db_path: str) -> None:
        self._db_path = db_path
        # Start writer server
        self._server = pasyn_sqlite_core.start_writer_server(db_path)
        # Create native hybrid connection
        self._conn = pasyn_sqlite_core.native_hybrid_connect(
            db_path, self._server.socket_path
        )

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._conn is not None
        sql_lower = sql.strip().upper()
        if sql_lower.startswith(("SELECT", "PRAGMA", "EXPLAIN", "WITH")):
            # Read operation - use local connection (sync)
            return self._conn.query_fetchall(sql, list(parameters))
        else:
            # Write operation - use native awaitable
            await self._conn.write_execute(sql, list(parameters))
            return []

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        assert self._conn is not None
        # Execute each set of parameters as a separate write
        for params in parameters:
            await self._conn.write_execute(sql, list(params))

    async def commit(self) -> None:
        assert self._conn is not None
        if self._in_transaction:
            await self._conn.write_commit()
            self._in_transaction = False

    async def begin_transaction(self) -> None:
        assert self._conn is not None
        await self._conn.write_begin()
        self._in_transaction = True

    async def rollback(self) -> None:
        assert self._conn is not None
        if self._in_transaction:
            await self._conn.write_rollback()
            self._in_transaction = False

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
        if self._conn:
            self._conn.close()
            self._conn = None
        if self._server:
            self._server.stop()
            self._server = None


# Factory function
def create_implementation(name: str, **kwargs: Any) -> BaseSQLiteImplementation:
    """Create an implementation by name."""
    implementations = {
        "main_thread": MainThreadSQLite,
        "single_thread": SingleThreadSQLite,
        "pasyn_pool": PasynPoolSQLite,
        "native_awaitable": NativeAwaitableSQLite,
    }
    if name not in implementations:
        raise ValueError(f"Unknown implementation: {name}")
    return implementations[name](**kwargs)
