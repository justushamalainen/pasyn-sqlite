"""Thread pool implementation with single writer and multiple readers with work stealing."""

from __future__ import annotations

import queue
import sqlite3
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum, auto
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
    Thread pool for SQLite with single writer and multiple readers.

    The writer thread handles all write operations (INSERT, UPDATE, DELETE, etc.)
    to ensure SQLite's write serialization.

    Reader threads handle SELECT queries with work-stealing for load balancing.
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
            "PRAGMA",  # Some PRAGMAs write, so be safe
        }
    )

    def __init__(
        self,
        database: str,
        num_readers: int = 3,
        check_same_thread: bool = False,
        **sqlite_kwargs: Any,
    ) -> None:
        """
        Initialize the thread pool.

        Args:
            database: Path to SQLite database file or ":memory:" for in-memory.
            num_readers: Number of reader threads (default: 3).
            check_same_thread: SQLite check_same_thread parameter.
            **sqlite_kwargs: Additional kwargs passed to sqlite3.connect().
        """
        self._database = database
        self._num_readers = num_readers
        self._sqlite_kwargs = {
            "check_same_thread": check_same_thread,
            **sqlite_kwargs,
        }

        self._closed = False
        self._shutdown = False

        # Writer thread state - use queue.Queue for efficient blocking
        self._writer_queue: queue.Queue[Task | None] = queue.Queue()
        self._writer_thread: threading.Thread | None = None
        self._writer_connection: sqlite3.Connection | None = None

        # Reader threads state - shared queue for all readers
        self._reader_queue: queue.Queue[Task | None] = queue.Queue()
        self._reader_threads: list[threading.Thread] = []
        self._reader_connections: list[sqlite3.Connection | None] = []

        # Event to signal writer is ready (for WAL mode setup)
        self._writer_ready = threading.Event()

        self._start_threads()

    def _start_threads(self) -> None:
        """Start all worker threads."""
        # Start writer thread
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="pasyn-sqlite-writer",
            daemon=True,
        )
        self._writer_thread.start()

        # Start reader threads
        for i in range(self._num_readers):
            self._reader_connections.append(None)
            thread = threading.Thread(
                target=self._reader_loop,
                args=(i,),
                name=f"pasyn-sqlite-reader-{i}",
                daemon=True,
            )
            self._reader_threads.append(thread)
            thread.start()

    def _create_writer_connection(self) -> sqlite3.Connection:
        """Create the writer connection and set up WAL mode."""
        conn = sqlite3.connect(self._database, **self._sqlite_kwargs)
        # Set busy timeout first to handle any potential locks
        conn.execute("PRAGMA busy_timeout=5000")
        # Enable WAL mode for better concurrent read performance
        # Only needs to be set once per database file
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _create_reader_connection(self) -> sqlite3.Connection:
        """Create a reader connection with retry for busy database."""
        max_retries = 3
        retry_delay = 0.1

        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self._database, **self._sqlite_kwargs)
                conn.execute("PRAGMA busy_timeout=5000")
                # WAL mode is already set by writer, just verify we can read
                conn.execute("SELECT 1")
                return conn
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise

        # Should not reach here, but satisfy type checker
        raise sqlite3.OperationalError("Failed to create reader connection")

    def _writer_loop(self) -> None:
        """Main loop for the writer thread."""
        self._writer_connection = self._create_writer_connection()
        # Signal that writer is ready and WAL mode is set
        self._writer_ready.set()

        while True:
            try:
                # Block waiting for task, with timeout for shutdown check
                task = self._writer_queue.get(timeout=0.1)
                if task is None:  # Shutdown signal
                    break
                self._execute_task(task, self._writer_connection)
            except queue.Empty:
                if self._shutdown:
                    break

        # Cleanup
        if self._writer_connection:
            self._writer_connection.close()

    def _reader_loop(self, reader_id: int) -> None:
        """Main loop for a reader thread."""
        # Wait for writer to be ready (WAL mode set)
        self._writer_ready.wait(timeout=5.0)

        connection = self._create_reader_connection()
        self._reader_connections[reader_id] = connection

        while True:
            try:
                # Block waiting for task, with timeout for shutdown check
                task = self._reader_queue.get(timeout=0.1)
                if task is None:  # Shutdown signal
                    # Put it back for other readers
                    self._reader_queue.put(None)
                    break
                self._execute_task(task, connection)
            except queue.Empty:
                if self._shutdown:
                    break

        # Cleanup
        connection.close()

    def _execute_task(
        self, task: Task, connection: sqlite3.Connection
    ) -> None:
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
        """
        Submit a write task to the writer thread.

        Args:
            func: Function to execute. First argument will be the connection.
            *args: Additional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            Future that will contain the result.

        Raises:
            PoolClosedError: If the pool is closed.
        """
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

        self._writer_queue.put(task)
        return future

    def submit_read(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """
        Submit a read task to the reader pool.

        The task will be picked up by the first available reader.

        Args:
            func: Function to execute. First argument will be the connection.
            *args: Additional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            Future that will contain the result.

        Raises:
            PoolClosedError: If the pool is closed.
        """
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

        self._reader_queue.put(task)
        return future

    def submit(
        self,
        func: Callable[..., T],
        sql: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """
        Submit a task, automatically routing to writer or reader.

        If sql is provided, it will be analyzed to determine if it's a
        read or write operation. Otherwise, defaults to write for safety.

        Args:
            func: Function to execute.
            sql: Optional SQL statement to analyze for routing.
            *args: Additional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            Future that will contain the result.
        """
        if sql is not None and self._is_read_operation(sql):
            return self.submit_read(func, *args, **kwargs)
        return self.submit_write(func, *args, **kwargs)

    def _is_read_operation(self, sql: str) -> bool:
        """
        Determine if a SQL statement is a read operation.

        Args:
            sql: SQL statement to analyze.

        Returns:
            True if the statement is a read operation.
        """
        # Normalize and get first word
        normalized = sql.strip().upper()

        # Handle WITH clauses (CTEs) - check if it ends with SELECT
        if normalized.startswith("WITH"):
            # Find the main query after the CTE
            # Simple heuristic: if there's no write keyword, it's a read
            for keyword in self.WRITE_KEYWORDS:
                if keyword in normalized:
                    return False
            return True

        # Get the first keyword
        first_word = normalized.split(None, 1)[0] if normalized else ""

        return first_word not in self.WRITE_KEYWORDS

    def close(self, wait: bool = True) -> None:
        """
        Close the pool and all connections.

        Args:
            wait: If True, wait for pending tasks to complete.
        """
        if self._closed:
            return

        self._closed = True
        self._shutdown = True

        # Send shutdown signals
        self._writer_queue.put(None)
        self._reader_queue.put(None)  # One None will cascade to all readers

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
