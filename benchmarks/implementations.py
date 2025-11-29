"""Different SQLite implementations for benchmarking."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Sequence

import pasyn_sqlite


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
    """

    def __init__(self) -> None:
        self._conn: sqlite3.Connection | None = None
        self._queue: deque[DBTask] = deque()
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._shutdown = threading.Event()
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

        while not self._shutdown.is_set():
            task = None
            with self._lock:
                if self._queue:
                    task = self._queue.popleft()

            if task is not None:
                try:
                    result = task.func(self._conn)
                    task.future.set_result(result)
                except Exception as e:
                    task.future.set_exception(e)
            else:
                self._event.wait(timeout=0.1)
                self._event.clear()

        if self._conn:
            self._conn.close()

    async def _submit(self, func: Callable[[sqlite3.Connection], Any]) -> Any:
        future: Future[Any] = Future()
        task = DBTask(func=func, args=(), future=future)

        with self._lock:
            self._queue.append(task)
        self._event.set()

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
        self._shutdown.set()
        self._event.set()
        if self._thread:
            self._thread.join(timeout=5.0)


class PasynSQLite(BaseSQLiteImplementation):
    """
    Legacy implementation using pasyn_sqlite.connect() API.
    """

    def __init__(self, num_readers: int = 3) -> None:
        self._num_readers = num_readers
        self._conn: pasyn_sqlite.Connection | None = None
        self._tx_lock: asyncio.Lock | None = None

    async def setup(self, db_path: str) -> None:
        self._tx_lock = asyncio.Lock()
        self._conn = await pasyn_sqlite.connect(
            db_path, num_readers=self._num_readers
        )

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._conn is not None
        cursor = await self._conn.execute(sql, parameters)
        return await cursor.fetchall()

    async def executemany(
        self, sql: str, parameters: Sequence[Sequence[Any]]
    ) -> None:
        assert self._conn is not None
        await self._conn.executemany(sql, parameters)

    async def commit(self) -> None:
        assert self._conn is not None
        await self._conn.commit()

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
        if self._conn:
            await self._conn.close()
            self._conn = None


class PasynPoolSQLite(BaseSQLiteImplementation):
    """
    New PasynPool implementation with bound_connection for transactions.

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


# Factory function
def create_implementation(name: str, **kwargs: Any) -> BaseSQLiteImplementation:
    """Create an implementation by name."""
    implementations = {
        "main_thread": MainThreadSQLite,
        "single_thread": SingleThreadSQLite,
        "pasyn": PasynSQLite,
        "pasyn_pool": PasynPoolSQLite,
    }
    if name not in implementations:
        raise ValueError(f"Unknown implementation: {name}")
    return implementations[name](**kwargs)
