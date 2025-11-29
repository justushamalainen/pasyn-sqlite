"""Async cursor implementation for pasyn-sqlite."""

from __future__ import annotations

import asyncio
import sqlite3
from typing import TYPE_CHECKING, Any, Iterator, Sequence

from .exceptions import ConnectionClosedError

if TYPE_CHECKING:
    from .connection import Connection


class Cursor:
    """
    Async cursor that mirrors sqlite3.Cursor interface.

    Results are fetched when execute is called and cached for
    subsequent fetch operations. This avoids cross-thread cursor
    state issues.
    """

    def __init__(self, connection: "Connection") -> None:
        """
        Initialize the cursor.

        Args:
            connection: The parent Connection object.
        """
        self._connection = connection
        self._rows: list[Any] = []
        self._row_index: int = 0
        self._description: tuple[tuple[str, ...], ...] | None = None
        self._rowcount: int = -1
        self._lastrowid: int | None = None
        self._arraysize: int = 1
        self._closed: bool = False

    def _check_closed(self) -> None:
        """Raise an error if the cursor is closed."""
        if self._closed:
            raise ConnectionClosedError("Cursor is closed")
        if self._connection.closed:
            raise ConnectionClosedError("Connection is closed")

    @staticmethod
    def _execute_in_thread(
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any],
        is_read: bool,
    ) -> tuple[list[Any], tuple[tuple[str, ...], ...] | None, int, int | None]:
        """
        Execute SQL in a thread and return results.

        Returns:
            Tuple of (rows, description, rowcount, lastrowid)
        """
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
        """
        Execute SQL with many parameter sets in a thread.

        Returns:
            Tuple of (rowcount, lastrowid)
        """
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
        """Execute a SQL script in a thread."""
        conn.executescript(script)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> "Cursor":
        """
        Execute a SQL statement.

        Args:
            sql: SQL statement to execute.
            parameters: Parameters for the SQL statement.

        Returns:
            This cursor for chaining.
        """
        self._check_closed()

        pool = self._connection._pool
        is_read = pool._is_read_operation(sql)

        # If we're in a transaction, ALL statements go to writer thread
        # to maintain transaction semantics (same connection required)
        in_transaction = self._connection._current_transaction is not None

        if in_transaction or not is_read:
            # Writer thread: transactions or write operations
            future = pool.submit_write(
                self._execute_in_thread, sql, parameters, is_read
            )
        else:
            # Reader thread: non-transactional reads
            future = pool.submit_read(
                self._execute_in_thread, sql, parameters, is_read
            )

        # Use wrap_future for efficient async waiting without blocking executor threads
        rows, description, rowcount, lastrowid = await asyncio.wrap_future(future)

        self._rows = rows
        self._row_index = 0
        self._description = description
        self._rowcount = rowcount
        self._lastrowid = lastrowid

        return self

    async def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> "Cursor":
        """
        Execute a SQL statement with multiple parameter sets.

        Args:
            sql: SQL statement to execute.
            parameters: Sequence of parameter sets.

        Returns:
            This cursor for chaining.
        """
        self._check_closed()

        pool = self._connection._pool
        future = pool.submit_write(
            self._executemany_in_thread, sql, parameters
        )

        rowcount, lastrowid = await asyncio.wrap_future(future)

        self._rows = []
        self._row_index = 0
        self._description = None
        self._rowcount = rowcount
        self._lastrowid = lastrowid

        return self

    async def executescript(self, script: str) -> "Cursor":
        """
        Execute a SQL script (multiple statements).

        Args:
            script: SQL script to execute.

        Returns:
            This cursor for chaining.
        """
        self._check_closed()

        pool = self._connection._pool
        future = pool.submit_write(self._executescript_in_thread, script)

        await asyncio.wrap_future(future)

        self._rows = []
        self._row_index = 0
        self._description = None
        self._rowcount = -1
        self._lastrowid = None

        return self

    async def fetchone(self) -> Any | None:
        """
        Fetch the next row from the result set.

        Returns:
            The next row, or None if no more rows.
        """
        self._check_closed()

        if self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        """
        Fetch the next set of rows.

        Args:
            size: Number of rows to fetch. Defaults to arraysize.

        Returns:
            List of rows.
        """
        self._check_closed()

        if size is None:
            size = self._arraysize

        end_index = min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index : end_index]
        self._row_index = end_index
        return rows

    async def fetchall(self) -> list[Any]:
        """
        Fetch all remaining rows.

        Returns:
            List of all remaining rows.
        """
        self._check_closed()

        rows = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return rows

    async def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._rows = []

    @property
    def description(self) -> tuple[tuple[str, ...], ...] | None:
        """Return the description of the columns."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Return the number of rows affected."""
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """Return the last inserted row ID."""
        return self._lastrowid

    @property
    def arraysize(self) -> int:
        """Return the default fetch size."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        """Set the default fetch size."""
        self._arraysize = value

    @property
    def closed(self) -> bool:
        """Return True if the cursor is closed."""
        return self._closed

    def __aiter__(self) -> "Cursor":
        """Return async iterator."""
        return self

    async def __anext__(self) -> Any:
        """Get next row from async iterator."""
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "Cursor":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()
