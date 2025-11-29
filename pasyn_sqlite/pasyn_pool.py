"""
PasynPool - Main async SQLite pool with SWMR architecture.

Provides:
- PasynPool: Main entry point with execute/executemany (auto-routes read/write)
- BoundConnection: Connection bound to writer thread for transactions
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
import warnings
from typing import Any, Sequence

from .exceptions import (
    ConnectionClosedError,
    PoolClosedError,
    TransactionCommandError,
)
from .pool import ThreadPool


# Pattern to detect transaction commands
_TRANSACTION_COMMANDS = frozenset({
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"
})

# Pattern for ROLLBACK TO (which is allowed - it's part of savepoint management)
_ROLLBACK_TO_PATTERN = re.compile(r"^\s*ROLLBACK\s+TO\b", re.IGNORECASE)


def _is_transaction_command(sql: str) -> tuple[bool, str]:
    """
    Check if SQL is a transaction command.

    Returns:
        Tuple of (is_transaction_command, command_name)
    """
    normalized = sql.strip().upper()

    # ROLLBACK TO is part of savepoint, but plain ROLLBACK is transaction
    if normalized.startswith("ROLLBACK"):
        if _ROLLBACK_TO_PATTERN.match(sql):
            return True, "ROLLBACK TO"
        return True, "ROLLBACK"

    first_word = normalized.split(None, 1)[0] if normalized else ""
    if first_word in _TRANSACTION_COMMANDS:
        return True, first_word

    return False, ""


class Cursor:
    """
    Async cursor that mirrors sqlite3.Cursor interface.

    Results are fetched eagerly and cached to avoid cross-thread issues.
    """

    def __init__(
        self,
        rows: list[Any] | None = None,
        description: tuple[tuple[str, ...], ...] | None = None,
        rowcount: int = -1,
        lastrowid: int | None = None,
    ) -> None:
        self._rows: list[Any] = rows or []
        self._row_index: int = 0
        self._description = description
        self._rowcount = rowcount
        self._lastrowid = lastrowid
        self._arraysize: int = 1
        self._closed: bool = False

    def _check_closed(self) -> None:
        if self._closed:
            raise ConnectionClosedError("Cursor is closed")

    async def fetchone(self) -> Any | None:
        """Fetch the next row."""
        self._check_closed()
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        """Fetch the next `size` rows."""
        self._check_closed()
        if size is None:
            size = self._arraysize
        end_index = min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index:end_index]
        self._row_index = end_index
        return rows

    async def fetchall(self) -> list[Any]:
        """Fetch all remaining rows."""
        self._check_closed()
        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows)
        return rows

    async def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._rows = []

    @property
    def description(self) -> tuple[tuple[str, ...], ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    @property
    def closed(self) -> bool:
        return self._closed

    def __aiter__(self) -> "Cursor":
        return self

    async def __anext__(self) -> Any:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


class BoundConnection:
    """
    Connection bound to the writer thread.

    Use this for transactions and operations that need to be on the same connection.
    Mirrors sqlite3.Connection interface.
    """

    def __init__(
        self,
        pool: "PasynPool",
        release_callback: Any,
    ) -> None:
        self._pool = pool
        self._thread_pool = pool._thread_pool
        self._release_callback = release_callback
        self._closed = False

    def _check_closed(self) -> None:
        if self._closed:
            raise ConnectionClosedError("BoundConnection is closed")
        if self._pool.closed:
            raise PoolClosedError("Pool is closed")

    @staticmethod
    def _execute_in_thread(
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any],
    ) -> tuple[list[Any], tuple[tuple[str, ...], ...] | None, int, int | None]:
        """Execute SQL and return results."""
        cursor = conn.execute(sql, parameters)
        # Always fetch for reads, empty list for writes
        try:
            rows = cursor.fetchall()
        except sqlite3.ProgrammingError:
            # Some statements don't return rows
            rows = []
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
        """Execute SQL with multiple parameter sets."""
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
        """Execute a SQL script."""
        conn.executescript(script)

    @staticmethod
    def _commit_in_thread(conn: sqlite3.Connection) -> None:
        conn.commit()

    @staticmethod
    def _rollback_in_thread(conn: sqlite3.Connection) -> None:
        conn.rollback()

    @staticmethod
    def _get_in_transaction(conn: sqlite3.Connection) -> bool:
        return conn.in_transaction

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> Cursor:
        """Execute SQL statement. Always uses writer thread."""
        self._check_closed()

        future = self._thread_pool.submit_write(
            self._execute_in_thread, sql, parameters
        )
        rows, description, rowcount, lastrowid = await asyncio.wrap_future(future)

        return Cursor(
            rows=rows,
            description=description,
            rowcount=rowcount,
            lastrowid=lastrowid,
        )

    async def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> Cursor:
        """Execute SQL with multiple parameter sets."""
        self._check_closed()

        future = self._thread_pool.submit_write(
            self._executemany_in_thread, sql, parameters
        )
        rowcount, lastrowid = await asyncio.wrap_future(future)

        return Cursor(rowcount=rowcount, lastrowid=lastrowid)

    async def executescript(self, script: str) -> Cursor:
        """Execute a SQL script (multiple statements)."""
        self._check_closed()

        future = self._thread_pool.submit_write(
            self._executescript_in_thread, script
        )
        await asyncio.wrap_future(future)

        return Cursor()

    async def commit(self) -> None:
        """Commit the current transaction."""
        self._check_closed()
        future = self._thread_pool.submit_write(self._commit_in_thread)
        await asyncio.wrap_future(future)

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        self._check_closed()
        future = self._thread_pool.submit_write(self._rollback_in_thread)
        await asyncio.wrap_future(future)

    @property
    def in_transaction(self) -> bool:
        """Return True if a transaction is active."""
        if self._closed or self._thread_pool._writer_connection is None:
            return False
        # This is a synchronous check - safe because it's just reading a flag
        return self._thread_pool._writer_connection.in_transaction

    async def close(self) -> None:
        """Close the bound connection and return it to the pool."""
        if self._closed:
            return

        # Check for uncommitted transaction
        if self.in_transaction:
            warnings.warn(
                "BoundConnection closed with uncommitted transaction, rolling back",
                stacklevel=2,
            )
            try:
                await self.rollback()
            except Exception:
                pass  # Best effort

        self._closed = True

        # Release the lock
        if self._release_callback:
            self._release_callback()

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> "BoundConnection":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # If exception occurred and we're in a transaction, rollback
        if exc_type is not None and self.in_transaction:
            try:
                await self.rollback()
            except Exception:
                pass
        await self.close()


class PasynPool:
    """
    Async SQLite pool with Single Writer Multiple Reader architecture.

    - execute/executemany: Auto-route to writer or reader based on SQL
    - bound_connection(): Get a connection bound to writer for transactions

    Transaction commands (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE) are
    not allowed on the pool directly. Use bound_connection() for transactions.
    """

    def __init__(
        self,
        database: str,
        num_readers: int = 3,
        **sqlite_kwargs: Any,
    ) -> None:
        self._database = database
        self._thread_pool = ThreadPool(
            database=database,
            num_readers=num_readers,
            **sqlite_kwargs,
        )
        self._closed = False
        self._bound_connection_lock = asyncio.Lock()

    def _check_closed(self) -> None:
        if self._closed:
            raise PoolClosedError("Pool is closed")

    def _check_transaction_command(self, sql: str) -> None:
        """Raise error if SQL is a transaction command."""
        is_tx, cmd = _is_transaction_command(sql)
        if is_tx:
            raise TransactionCommandError(
                f"'{cmd}' is not allowed on Pool. Use pool.bound_connection() for transactions."
            )

    @staticmethod
    def _execute_in_thread(
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any],
        is_read: bool,
    ) -> tuple[list[Any], tuple[tuple[str, ...], ...] | None, int, int | None]:
        """Execute SQL and return results."""
        cursor = conn.execute(sql, parameters)
        rows = cursor.fetchall() if is_read else []
        description = cursor.description
        rowcount = cursor.rowcount
        lastrowid = cursor.lastrowid
        cursor.close()
        # Auto-commit writes to release locks (pool.execute doesn't use transactions)
        if not is_read:
            conn.commit()
        return rows, description, rowcount, lastrowid

    @staticmethod
    def _executemany_in_thread(
        conn: sqlite3.Connection,
        sql: str,
        parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> tuple[int, int | None]:
        """Execute SQL with multiple parameter sets."""
        cursor = conn.executemany(sql, parameters)
        rowcount = cursor.rowcount
        lastrowid = cursor.lastrowid
        cursor.close()
        # Auto-commit to release locks (pool.executemany doesn't use transactions)
        conn.commit()
        return rowcount, lastrowid

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> Cursor:
        """
        Execute a SQL statement, automatically routing to writer or reader.

        Transaction commands (BEGIN, COMMIT, ROLLBACK, etc.) are not allowed.
        Use bound_connection() for transactions.
        """
        self._check_closed()
        self._check_transaction_command(sql)

        is_read = self._thread_pool._is_read_operation(sql)

        if is_read:
            future = self._thread_pool.submit_read(
                self._execute_in_thread, sql, parameters, is_read
            )
        else:
            future = self._thread_pool.submit_write(
                self._execute_in_thread, sql, parameters, is_read
            )

        rows, description, rowcount, lastrowid = await asyncio.wrap_future(future)

        return Cursor(
            rows=rows,
            description=description,
            rowcount=rowcount,
            lastrowid=lastrowid,
        )

    async def executemany(
        self,
        sql: str,
        parameters: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> Cursor:
        """
        Execute SQL with multiple parameter sets.

        Transaction commands are not allowed.
        """
        self._check_closed()
        self._check_transaction_command(sql)

        future = self._thread_pool.submit_write(
            self._executemany_in_thread, sql, parameters
        )
        rowcount, lastrowid = await asyncio.wrap_future(future)

        return Cursor(rowcount=rowcount, lastrowid=lastrowid)

    async def bound_connection(self) -> BoundConnection:
        """
        Acquire a connection bound to the writer thread.

        Use this for transactions. The connection is exclusive - only one
        bound_connection can be active at a time.

        Example:
            async with pool.bound_connection() as conn:
                await conn.execute("BEGIN")
                await conn.execute("INSERT INTO users VALUES (?)", (1,))
                await conn.execute("COMMIT")
        """
        self._check_closed()

        await self._bound_connection_lock.acquire()

        return BoundConnection(
            pool=self,
            release_callback=self._bound_connection_lock.release,
        )

    async def close(self) -> None:
        """Close the pool and all connections."""
        if self._closed:
            return
        self._closed = True
        self._thread_pool.close(wait=True)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def num_readers(self) -> int:
        return self._thread_pool.num_readers

    async def __aenter__(self) -> "PasynPool":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


async def create_pool(
    database: str,
    *,
    num_readers: int = 3,
    **sqlite_kwargs: Any,
) -> PasynPool:
    """
    Create a new async SQLite pool.

    Args:
        database: Path to SQLite database or ":memory:".
        num_readers: Number of reader threads (default: 3).
        **sqlite_kwargs: Additional arguments passed to sqlite3.connect().

    Returns:
        A PasynPool instance.

    Example:
        pool = await pasyn_sqlite.create_pool("mydb.sqlite")

        # Simple queries
        cursor = await pool.execute("SELECT * FROM users")
        rows = await cursor.fetchall()

        # Transactions
        async with pool.bound_connection() as conn:
            await conn.execute("BEGIN")
            await conn.execute("INSERT INTO users VALUES (?)", (1,))
            await conn.execute("COMMIT")
    """
    return PasynPool(
        database=database,
        num_readers=num_readers,
        **sqlite_kwargs,
    )
