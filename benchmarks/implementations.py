"""Different SQLite implementations for benchmarking."""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Sequence
from typing import Any, Callable

import apsw
import pasyn_sqlite_core
import pysqlite3


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
    async def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
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
        self, operations: Callable[[BaseSQLiteImplementation], Coroutine[Any, Any, Any]]
    ) -> None:
        """Run operations within a transaction."""
        await self.begin_transaction()
        try:
            await operations(self)
            await self.commit()
        except Exception:
            await self.rollback()
            raise

    async def __aenter__(self) -> BaseSQLiteImplementation:
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
        # Use isolation_level=None for autocommit mode - each statement is its own transaction
        self._conn = sqlite3.connect(db_path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._conn is not None
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchall()

    async def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
        assert self._conn is not None
        self._conn.executemany(sql, parameters)

    async def commit(self) -> None:
        assert self._conn is not None
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class APSWMainThreadSQLite(BaseSQLiteImplementation):
    """
    SQLite using APSW library running in the main thread (synchronous).

    APSW (Another Python SQLite Wrapper) provides a thin wrapper around SQLite.
    This is a baseline for comparing APSW with sqlite3.
    """

    def __init__(self) -> None:
        self._conn: apsw.Connection | None = None
        self._in_transaction: bool = False

    async def setup(self, db_path: str) -> None:
        self._conn = apsw.Connection(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._conn is not None
        cursor = self._conn.execute(sql, parameters)
        return list(cursor)

    async def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
        assert self._conn is not None
        self._conn.executemany(sql, parameters)

    async def begin_transaction(self) -> None:
        assert self._conn is not None
        if not self._in_transaction:
            self._conn.execute("BEGIN")
            self._in_transaction = True

    async def commit(self) -> None:
        assert self._conn is not None
        if self._in_transaction:
            self._conn.execute("COMMIT")
            self._in_transaction = False

    async def rollback(self) -> None:
        assert self._conn is not None
        if self._in_transaction:
            self._conn.execute("ROLLBACK")
            self._in_transaction = False

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class Pysqlite3MainThreadSQLite(BaseSQLiteImplementation):
    """
    SQLite using pysqlite3 library running in the main thread (synchronous).

    pysqlite3 is a standalone package providing sqlite3 module with newer SQLite versions.
    This is a baseline for comparing pysqlite3 with sqlite3.
    """

    def __init__(self) -> None:
        self._conn: pysqlite3.Connection | None = None

    async def setup(self, db_path: str) -> None:
        # Use isolation_level=None for autocommit mode - each statement is its own transaction
        self._conn = pysqlite3.connect(db_path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._conn is not None
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchall()

    async def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
        assert self._conn is not None
        self._conn.executemany(sql, parameters)

    async def commit(self) -> None:
        assert self._conn is not None
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class MultiplexedSQLite(BaseSQLiteImplementation):
    """
    Multiplexed client implementation using Rust thread-safe client.

    This implementation:
    - Uses a single shared connection for all operations
    - Automatically batches concurrent requests
    - Lock-free request submission (minimal contention)
    - Releases GIL during I/O operations
    - Uses local SQLite connection for reads (sync, fast)
    """

    def __init__(self) -> None:
        self._server: pasyn_sqlite_core.WriterServerHandle | None = None
        self._client: pasyn_sqlite_core.MultiplexedClient | None = None
        self._read_conn: pasyn_sqlite_core.Connection | None = None
        self._db_path: str | None = None
        self._in_transaction: bool = False

    async def setup(self, db_path: str) -> None:
        self._db_path = db_path
        # Start writer server
        self._server = pasyn_sqlite_core.start_writer_server(db_path)
        # Create multiplexed client for writes
        self._client = pasyn_sqlite_core.MultiplexedClient(self._server.socket_path)
        # Create local read-only connection for reads
        self._read_conn = pasyn_sqlite_core.connect(db_path, pasyn_sqlite_core.OpenFlags.readonly())

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> list[Any]:
        assert self._client is not None
        assert self._read_conn is not None
        sql_lower = sql.strip().upper()
        if sql_lower.startswith(("SELECT", "PRAGMA", "EXPLAIN", "WITH")):
            # Read operation - use local connection (sync)
            return self._read_conn.execute_fetchall(sql, list(parameters))
        else:
            # Write operation - use multiplexed client
            # Note: this is synchronous but releases GIL during I/O
            self._client.execute(sql, list(parameters))
            return []

    async def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
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
        self, operations: Callable[[BaseSQLiteImplementation], Coroutine[Any, Any, Any]]
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
        if self._read_conn:
            self._read_conn.close()
            self._read_conn = None
        self._client = None  # No explicit close needed
        if self._server:
            self._server.stop()
            self._server = None


# Factory function
def create_implementation(name: str, **kwargs: Any) -> BaseSQLiteImplementation:
    """Create an implementation by name."""
    implementations = {
        "main_thread": MainThreadSQLite,
        "apsw_main_thread": APSWMainThreadSQLite,
        "pysqlite3_main_thread": Pysqlite3MainThreadSQLite,
        "multiplexed": MultiplexedSQLite,
    }
    if name not in implementations:
        raise ValueError(f"Unknown implementation: {name}")
    return implementations[name](**kwargs)
