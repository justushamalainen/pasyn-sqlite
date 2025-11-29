"""Different SQLite implementations for benchmarking."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Sequence

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

    async def setup(self, db_path: str) -> None:
        self._db_path = db_path
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

    async def close(self) -> None:
        self._shutdown.set()
        self._event.set()
        if self._thread:
            self._thread.join(timeout=5.0)


class PasynSQLite(BaseSQLiteImplementation):
    """
    Our implementation with single writer + multiple readers with work stealing.
    """

    def __init__(self, num_readers: int = 3) -> None:
        self._num_readers = num_readers
        self._conn: pasyn_sqlite.Connection | None = None

    async def setup(self, db_path: str) -> None:
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

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None


# Factory function
def create_implementation(name: str, **kwargs: Any) -> BaseSQLiteImplementation:
    """Create an implementation by name."""
    implementations = {
        "main_thread": MainThreadSQLite,
        "single_thread": SingleThreadSQLite,
        "pasyn": PasynSQLite,
    }
    if name not in implementations:
        raise ValueError(f"Unknown implementation: {name}")
    return implementations[name](**kwargs)
