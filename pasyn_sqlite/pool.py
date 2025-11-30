"""
Thread pool with Single Writer Multiple Reader (SWMR) architecture.

Uses blocking Queue.get() for zero-polling, efficient thread coordination.
Each reader has its own queue to eliminate lock contention.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum, auto
from queue import Queue
from typing import Any, Callable, TypeVar

from .exceptions import PoolClosedError

T = TypeVar("T")


class TaskType(Enum):
    """Type of database task."""

    READ = auto()
    WRITE = auto()


@dataclass
class Task:
    """A task to be executed by the pool."""

    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    future: Future[Any]
    task_type: TaskType


class ThreadPool:
    """
    Thread pool for SQLite with SWMR architecture.

    - Single writer thread: Handles all write operations (INSERT, UPDATE, etc.)
    - Reader thread(s): Handle SELECT queries

    Uses separate queue per reader to eliminate lock contention.
    Tasks are distributed round-robin to reader queues.
    """

    # SQL keywords that indicate write operations
    WRITE_KEYWORDS = frozenset(
        {
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "REPLACE",
            "TRUNCATE",
            "ATTACH",
            "DETACH",
            "REINDEX",
            "VACUUM",
            "ANALYZE",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
            "PRAGMA",
        }
    )

    def __init__(
        self,
        database: str,
        num_readers: int = 1,
        check_same_thread: bool = False,
        **sqlite_kwargs: Any,
    ) -> None:
        """
        Initialize the thread pool.

        Args:
            database: Path to SQLite database file or ":memory:" for in-memory.
            num_readers: Number of reader threads (default: 1).
            check_same_thread: SQLite check_same_thread parameter.
            **sqlite_kwargs: Additional kwargs passed to sqlite3.connect().
        """
        # Handle in-memory databases - use shared cache so all connections
        # see the same data
        if database == ":memory:":
            database = "file::memory:?cache=shared"
            sqlite_kwargs.setdefault("uri", True)

        self._database = database
        self._num_readers = num_readers
        self._sqlite_kwargs = {
            "check_same_thread": check_same_thread,
            **sqlite_kwargs,
        }

        self._closed = False

        # Write queue - blocking Queue, no polling
        self._write_queue: Queue[Task | None] = Queue()

        # Separate queue per reader - eliminates lock contention!
        self._reader_queues: list[Queue[Task | None]] = [
            Queue() for _ in range(num_readers)
        ]
        self._next_reader = 0  # Round-robin counter

        # Writer thread
        self._writer_thread: threading.Thread | None = None
        self._writer_connection: sqlite3.Connection | None = None
        self._writer_ready = threading.Event()

        # Reader threads (simple threads, no executor overhead)
        self._reader_threads: list[threading.Thread] = []
        self._reader_connections: list[sqlite3.Connection] = []

        self._start_threads()

    def _start_threads(self) -> None:
        """Start writer thread and reader threads."""
        # Start single writer thread
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="pasyn-sqlite-writer",
            daemon=True,
        )
        self._writer_thread.start()

        # Start reader threads, each with its own queue
        for i in range(self._num_readers):
            thread = threading.Thread(
                target=self._reader_loop,
                args=(i, self._reader_queues[i]),
                name=f"pasyn-sqlite-reader-{i}",
                daemon=True,
            )
            self._reader_threads.append(thread)
            thread.start()

    def _create_writer_connection(self) -> sqlite3.Connection:
        """Create the writer connection and set up WAL mode."""
        conn = sqlite3.connect(self._database, **self._sqlite_kwargs)
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _create_reader_connection(self) -> sqlite3.Connection:
        """Create a reader connection with retry for busy database."""
        max_retries = 3
        retry_delay = 0.05

        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self._database, **self._sqlite_kwargs)
                conn.execute("PRAGMA busy_timeout=5000")
                conn.execute("SELECT 1")  # Verify connection works
                return conn
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise

        raise sqlite3.OperationalError("Failed to create reader connection")

    def _writer_loop(self) -> None:
        """Main loop for the single writer thread."""
        self._writer_connection = self._create_writer_connection()
        self._writer_ready.set()

        while True:
            # Blocks until task available - no polling!
            task = self._write_queue.get()
            if task is None:  # Shutdown sentinel
                break
            self._execute_task(task, self._writer_connection)

        if self._writer_connection:
            self._writer_connection.close()

    def _reader_loop(self, reader_id: int, queue: Queue[Task | None]) -> None:
        """Main loop for a reader thread with its own dedicated queue."""
        # Wait for writer to establish WAL mode
        self._writer_ready.wait(timeout=5.0)

        conn = self._create_reader_connection()
        self._reader_connections.append(conn)

        while True:
            # Blocks on own queue - no contention with other readers!
            task = queue.get()
            if task is None:  # Shutdown sentinel
                break
            self._execute_task(task, conn)

        conn.close()

    def _execute_task(self, task: Task, connection: sqlite3.Connection) -> None:
        """Execute a task and set its result or exception."""
        if task.future.cancelled():
            return

        try:
            result = task.func(connection, *task.args, **task.kwargs)
            task.future.set_result(result)
        except Exception as e:
            task.future.set_exception(e)

    def submit_write(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Submit a write task to the writer thread."""
        if self._closed:
            raise PoolClosedError("Pool is closed")

        future: Future[T] = Future()
        task = Task(
            func=func,
            args=args,
            kwargs=kwargs,
            future=future,
            task_type=TaskType.WRITE,
        )

        self._write_queue.put(task)
        return future

    def submit_read(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Submit a read task to a reader thread (round-robin)."""
        if self._closed:
            raise PoolClosedError("Pool is closed")

        future: Future[T] = Future()
        task = Task(
            func=func,
            args=args,
            kwargs=kwargs,
            future=future,
            task_type=TaskType.READ,
        )

        # Round-robin distribution to reader queues
        queue = self._reader_queues[self._next_reader]
        self._next_reader = (self._next_reader + 1) % self._num_readers
        queue.put(task)

        return future

    def submit(
        self,
        func: Callable[..., T],
        sql: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Submit a task, automatically routing to writer or reader."""
        if sql is not None and self._is_read_operation(sql):
            return self.submit_read(func, *args, **kwargs)
        return self.submit_write(func, *args, **kwargs)

    def _is_read_operation(self, sql: str) -> bool:
        """Determine if a SQL statement is a read operation."""
        normalized = sql.strip().upper()

        if normalized.startswith("WITH"):
            for keyword in self.WRITE_KEYWORDS:
                if keyword in normalized:
                    return False
            return True

        first_word = normalized.split(None, 1)[0] if normalized else ""
        return first_word not in self.WRITE_KEYWORDS

    def close(self, wait: bool = True) -> None:
        """Close the pool and all connections."""
        if self._closed:
            return

        self._closed = True

        # Send shutdown sentinels
        self._write_queue.put(None)
        for queue in self._reader_queues:
            queue.put(None)

        if wait:
            if self._writer_thread:
                self._writer_thread.join(timeout=5.0)

            for thread in self._reader_threads:
                thread.join(timeout=5.0)

    def __enter__(self) -> "ThreadPool":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        """Return True if the pool is closed."""
        return self._closed

    @property
    def num_readers(self) -> int:
        """Return the number of reader threads."""
        return self._num_readers
