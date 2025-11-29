"""Async connection implementation for pasyn-sqlite."""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, Callable, Sequence

from .cursor import Cursor
from .exceptions import ConnectionClosedError
from .pool import ThreadPool


class Connection:
    """
    Async connection that mirrors sqlite3.Connection interface.

    Uses a thread pool with a single writer thread and multiple
    reader threads with work-stealing for optimal performance.
    """

    def __init__(
        self,
        database: str,
        num_readers: int = 3,
        isolation_level: str | None = "",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the connection.

        Args:
            database: Path to SQLite database or ":memory:".
            num_readers: Number of reader threads (default: 3).
            isolation_level: Transaction isolation level.
            **kwargs: Additional arguments passed to sqlite3.connect().
        """
        self._database = database
        self._isolation_level = isolation_level
        self._pool = ThreadPool(
            database=database,
            num_readers=num_readers,
            isolation_level=isolation_level,
            **kwargs,
        )
        self._closed = False

    def _check_closed(self) -> None:
        """Raise an error if the connection is closed."""
        if self._closed:
            raise ConnectionClosedError("Connection is closed")

    @staticmethod
    def _commit_in_thread(conn: sqlite3.Connection) -> None:
        """Commit in thread."""
        conn.commit()

    @staticmethod
    def _rollback_in_thread(conn: sqlite3.Connection) -> None:
        """Rollback in thread."""
        conn.rollback()

    @staticmethod
    def _execute_in_thread(
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any],
        is_read: bool,
    ) -> tuple[list[Any], tuple[tuple[str, ...], ...] | None, int, int | None]:
        """Execute and return results."""
        cursor = conn.execute(sql, parameters)
        rows = cursor.fetchall() if is_read else []
        description = cursor.description
        rowcount = cursor.rowcount
        lastrowid = cursor.lastrowid
        cursor.close()
        return rows, description, rowcount, lastrowid

    @staticmethod
    def _executemany_in_thread(
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> tuple[int, int | None]:
        """Execute many and return results."""
        cursor = conn.executemany(sql, parameters)
        rowcount = cursor.rowcount
        lastrowid = cursor.lastrowid
        cursor.close()
        return rowcount, lastrowid

    @staticmethod
    def _executescript_in_thread(
        conn: sqlite3.Connection,
        script: str,
    ) -> None:
        """Execute script in thread."""
        conn.executescript(script)

    @staticmethod
    def _create_function_in_thread(
        conn: sqlite3.Connection,
        name: str,
        num_params: int,
        func: Callable[..., Any],
        deterministic: bool,
    ) -> None:
        """Create a user-defined function in thread."""
        conn.create_function(name, num_params, func, deterministic=deterministic)

    @staticmethod
    def _set_trace_callback_in_thread(
        conn: sqlite3.Connection,
        callback: Callable[[str], None] | None,
    ) -> None:
        """Set trace callback in thread."""
        conn.set_trace_callback(callback)

    async def cursor(self) -> Cursor:
        """
        Create a new cursor.

        Returns:
            A new Cursor object.
        """
        self._check_closed()
        return Cursor(self)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> Cursor:
        """
        Execute a SQL statement and return a cursor.

        Args:
            sql: SQL statement to execute.
            parameters: Parameters for the SQL statement.

        Returns:
            Cursor with results.
        """
        self._check_closed()

        cursor = Cursor(self)
        await cursor.execute(sql, parameters)
        return cursor

    async def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> Cursor:
        """
        Execute a SQL statement with multiple parameter sets.

        Args:
            sql: SQL statement to execute.
            parameters: Sequence of parameter sets.

        Returns:
            Cursor with results.
        """
        self._check_closed()

        cursor = Cursor(self)
        await cursor.executemany(sql, parameters)
        return cursor

    async def executescript(self, script: str) -> Cursor:
        """
        Execute a SQL script (multiple statements).

        Args:
            script: SQL script to execute.

        Returns:
            Cursor.
        """
        self._check_closed()

        cursor = Cursor(self)
        await cursor.executescript(script)
        return cursor

    async def commit(self) -> None:
        """Commit the current transaction."""
        self._check_closed()

        future = self._pool.submit_write(self._commit_in_thread)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, future.result)

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        self._check_closed()

        future = self._pool.submit_write(self._rollback_in_thread)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, future.result)

    async def close(self) -> None:
        """Close the connection and the thread pool."""
        if self._closed:
            return

        self._closed = True
        self._pool.close(wait=True)

    async def create_function(
        self,
        name: str,
        num_params: int,
        func: Callable[..., Any],
        *,
        deterministic: bool = False,
    ) -> None:
        """
        Create a user-defined SQL function.

        Args:
            name: Name of the function in SQL.
            num_params: Number of parameters the function accepts.
            func: Python function to call.
            deterministic: If True, function always returns same output for same input.
        """
        self._check_closed()

        # Create function on all connections (writer and readers)
        future = self._pool.submit_write(
            self._create_function_in_thread, name, num_params, func, deterministic
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, future.result)

    async def set_trace_callback(
        self, callback: Callable[[str], None] | None
    ) -> None:
        """
        Set a trace callback for debugging.

        Args:
            callback: Function called for each SQL statement, or None to disable.
        """
        self._check_closed()

        future = self._pool.submit_write(
            self._set_trace_callback_in_thread, callback
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, future.result)

    @property
    def closed(self) -> bool:
        """Return True if the connection is closed."""
        return self._closed

    @property
    def isolation_level(self) -> str | None:
        """Return the isolation level."""
        return self._isolation_level

    @property
    def in_transaction(self) -> bool:
        """
        Return True if a transaction is active.

        Note: This is approximate as it's based on the writer connection.
        """
        if self._closed or self._pool._writer_connection is None:
            return False
        return self._pool._writer_connection.in_transaction

    @property
    def row_factory(self) -> Any:
        """Get the row factory (from writer connection)."""
        if self._pool._writer_connection is None:
            return None
        return self._pool._writer_connection.row_factory

    @row_factory.setter
    def row_factory(self, factory: Any) -> None:
        """
        Set the row factory on all connections.

        Note: This is a synchronous operation that modifies thread state.
        For best results, set this before executing any queries.
        """
        if self._pool._writer_connection is not None:
            self._pool._writer_connection.row_factory = factory

        for conn in self._pool._reader_connections:
            if conn is not None:
                conn.row_factory = factory

    async def __aenter__(self) -> "Connection":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()


async def connect(
    database: str,
    *,
    num_readers: int = 3,
    isolation_level: str | None = "",
    **kwargs: Any,
) -> Connection:
    """
    Create and return a new async connection.

    This is the main entry point for creating connections, mirroring
    sqlite3.connect().

    Args:
        database: Path to SQLite database or ":memory:".
        num_readers: Number of reader threads (default: 3).
        isolation_level: Transaction isolation level.
        **kwargs: Additional arguments passed to sqlite3.connect().

    Returns:
        An async Connection object.

    Example:
        async with await pasyn_sqlite.connect("mydb.sqlite") as conn:
            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
    """
    return Connection(
        database=database,
        num_readers=num_readers,
        isolation_level=isolation_level,
        **kwargs,
    )
