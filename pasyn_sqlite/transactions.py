"""Transaction support for pasyn-sqlite."""

from __future__ import annotations

import asyncio
import sqlite3
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from .exceptions import (
    NoActiveTransactionError,
    TransactionAlreadyActiveError,
)

if TYPE_CHECKING:
    from .connection import Connection


class IsolationLevel(Enum):
    """SQLite transaction isolation levels."""

    DEFERRED = auto()  # Default - acquires locks lazily
    IMMEDIATE = auto()  # Acquires reserved lock immediately
    EXCLUSIVE = auto()  # Acquires exclusive lock immediately


@dataclass
class TransactionContext:
    """
    Tracks the state of an active transaction.

    All statements within this transaction must execute on the same
    connection (the writer connection).
    """

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    isolation: IsolationLevel = IsolationLevel.DEFERRED
    started_at: float = field(default_factory=time.time)
    nesting_depth: int = 0
    savepoint_stack: list[str] = field(default_factory=list)
    owner_task: asyncio.Task[Any] | None = None  # Track which task owns this transaction

    def create_savepoint_name(self) -> str:
        """Generate a unique savepoint name."""
        name = f"sp_{self.id}_{len(self.savepoint_stack)}"
        self.savepoint_stack.append(name)
        return name

    def pop_savepoint(self) -> str | None:
        """Remove and return the last savepoint name."""
        if self.savepoint_stack:
            return self.savepoint_stack.pop()
        return None


class TransactionQueue:
    """
    Queue for serializing transaction requests on the writer thread.

    Only one transaction can be active at a time on the writer connection.
    Other transaction requests wait in queue.
    """

    def __init__(self) -> None:
        self._active: TransactionContext | None = None
        self._waiting: deque[asyncio.Event] = deque()
        self._lock = asyncio.Lock()

    @property
    def has_active(self) -> bool:
        """Return True if a transaction is currently active."""
        return self._active is not None

    @property
    def active_context(self) -> TransactionContext | None:
        """Return the active transaction context, if any."""
        return self._active

    async def acquire(self, context: TransactionContext) -> None:
        """
        Acquire a transaction slot.

        Blocks if another transaction is active.
        """
        async with self._lock:
            if self._active is None:
                self._active = context
                return

            # Another transaction is active - wait in queue
            event = asyncio.Event()
            self._waiting.append(event)

        # Wait outside the lock
        await event.wait()

        # Now we can set ourselves as active
        async with self._lock:
            self._active = context

    async def release(self) -> None:
        """
        Release the transaction slot.

        Wakes up the next waiting transaction, if any.
        """
        async with self._lock:
            self._active = None

            if self._waiting:
                next_event = self._waiting.popleft()
                next_event.set()


class Transaction:
    """
    Async context manager for explicit transactions.

    Usage:
        async with conn.transaction() as tx:
            await conn.execute("INSERT INTO ...")
            await conn.execute("UPDATE ...")
            # Auto-commits on success, rollbacks on exception

        # With isolation level:
        async with conn.transaction(isolation=IsolationLevel.IMMEDIATE):
            ...

        # Nested (uses savepoints):
        async with conn.transaction():
            await conn.execute("INSERT ...")
            async with conn.transaction():  # Creates savepoint
                await conn.execute("INSERT ...")
    """

    def __init__(
        self,
        connection: "Connection",
        isolation: IsolationLevel = IsolationLevel.DEFERRED,
    ) -> None:
        self._connection = connection
        self._isolation = isolation
        self._context: TransactionContext | None = None
        self._is_nested = False
        self._savepoint_name: str | None = None

    async def __aenter__(self) -> "Transaction":
        """Start the transaction."""
        conn = self._connection
        pool = conn._pool
        queue = conn._transaction_queue
        current_task = asyncio.current_task()

        # Check if we're nesting inside an existing transaction
        # Only nest if the SAME task owns the existing transaction
        # Different tasks should serialize (wait for their turn)
        existing = conn._current_transaction
        if existing is not None and existing.owner_task is current_task:
            # Nested transaction from same task - use savepoint
            self._is_nested = True
            self._context = existing
            existing.nesting_depth += 1
            self._savepoint_name = existing.create_savepoint_name()

            # Execute SAVEPOINT on writer thread
            future = pool.submit_write(
                self._execute_sql, f"SAVEPOINT {self._savepoint_name}"
            )
            await asyncio.wrap_future(future)
        else:
            # New transaction - acquire slot first (will wait if another is active)
            self._context = TransactionContext(
                isolation=self._isolation,
                owner_task=current_task,
            )
            await queue.acquire(self._context)

            # Set the context on the connection AFTER acquiring the slot
            conn._current_transaction = self._context

            # Execute BEGIN on writer thread
            begin_sql = f"BEGIN {self._isolation.name}"
            future = pool.submit_write(self._execute_sql, begin_sql)
            await asyncio.wrap_future(future)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """End the transaction - commit on success, rollback on exception."""
        conn = self._connection
        pool = conn._pool

        try:
            if self._is_nested:
                # Nested - handle savepoint
                if exc_type is not None:
                    # Rollback to savepoint
                    future = pool.submit_write(
                        self._execute_sql,
                        f"ROLLBACK TO {self._savepoint_name}",
                    )
                    await asyncio.wrap_future(future)
                else:
                    # Release savepoint (commits nested changes)
                    future = pool.submit_write(
                        self._execute_sql,
                        f"RELEASE {self._savepoint_name}",
                    )
                    await asyncio.wrap_future(future)

                if self._context:
                    self._context.nesting_depth -= 1
                    self._context.pop_savepoint()
            else:
                # Top-level transaction
                if exc_type is not None:
                    # Rollback on exception
                    future = pool.submit_write(self._execute_sql, "ROLLBACK")
                    await asyncio.wrap_future(future)
                else:
                    # Commit on success
                    future = pool.submit_write(self._execute_sql, "COMMIT")
                    await asyncio.wrap_future(future)

                # Clear transaction context and release queue slot
                conn._current_transaction = None
                await conn._transaction_queue.release()
        except Exception:
            # If commit/rollback fails, still try to clean up
            if not self._is_nested:
                conn._current_transaction = None
                await conn._transaction_queue.release()
            raise

        # Don't suppress exceptions
        return False

    @staticmethod
    def _execute_sql(conn: sqlite3.Connection, sql: str) -> None:
        """Execute SQL on the connection."""
        conn.execute(sql)

    @property
    def context(self) -> TransactionContext | None:
        """Return the transaction context."""
        return self._context

    @property
    def is_nested(self) -> bool:
        """Return True if this is a nested transaction (savepoint)."""
        return self._is_nested


class Savepoint:
    """
    Async context manager for explicit savepoints.

    Usage:
        async with conn.transaction():
            await conn.execute("INSERT ...")
            async with conn.savepoint("my_savepoint"):
                await conn.execute("INSERT ...")
                # Can rollback independently
    """

    def __init__(self, connection: "Connection", name: str | None = None) -> None:
        self._connection = connection
        self._name = name
        self._actual_name: str | None = None

    async def __aenter__(self) -> "Savepoint":
        """Create the savepoint."""
        conn = self._connection
        pool = conn._pool
        ctx = conn._current_transaction

        if ctx is None:
            raise NoActiveTransactionError(
                "Cannot create savepoint outside of a transaction"
            )

        # Generate name if not provided
        if self._name:
            self._actual_name = self._name
            ctx.savepoint_stack.append(self._name)
        else:
            self._actual_name = ctx.create_savepoint_name()

        future = pool.submit_write(
            Transaction._execute_sql, f"SAVEPOINT {self._actual_name}"
        )
        await asyncio.wrap_future(future)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """Release or rollback the savepoint."""
        conn = self._connection
        pool = conn._pool
        ctx = conn._current_transaction

        if ctx is None:
            return False

        if exc_type is not None:
            # Rollback to savepoint
            future = pool.submit_write(
                Transaction._execute_sql,
                f"ROLLBACK TO {self._actual_name}",
            )
            await asyncio.wrap_future(future)
        else:
            # Release savepoint
            future = pool.submit_write(
                Transaction._execute_sql,
                f"RELEASE {self._actual_name}",
            )
            await asyncio.wrap_future(future)

        # Remove from stack
        if self._actual_name in ctx.savepoint_stack:
            ctx.savepoint_stack.remove(self._actual_name)

        return False

    @property
    def name(self) -> str | None:
        """Return the savepoint name."""
        return self._actual_name
