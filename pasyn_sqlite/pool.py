"""Thread pool implementation with single writer and multiple readers with work stealing."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
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


@dataclass
class ReaderState:
    """State for a single reader thread."""

    local_queue: deque[Task] = field(default_factory=deque)
    lock: threading.Lock = field(default_factory=threading.Lock)
    thread: threading.Thread | None = None


class ThreadPool:
    """
    Thread pool for SQLite with single writer and multiple readers.

    The writer thread handles all write operations (INSERT, UPDATE, DELETE, etc.)
    to ensure SQLite's write serialization.

    Reader threads handle SELECT queries with work-stealing for load balancing.
    When a reader's local queue is empty, it attempts to steal work from other
    readers before checking the shared queue.
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
        self._shutdown_event = threading.Event()

        # Writer thread state
        self._writer_queue: deque[Task] = deque()
        self._writer_lock = threading.Lock()
        self._writer_event = threading.Event()
        self._writer_thread: threading.Thread | None = None
        self._writer_connection: sqlite3.Connection | None = None

        # Reader threads state
        self._readers: list[ReaderState] = []
        self._reader_connections: list[sqlite3.Connection | None] = []
        self._shared_read_queue: deque[Task] = deque()
        self._shared_read_lock = threading.Lock()
        self._reader_event = threading.Event()

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
            state = ReaderState()
            self._readers.append(state)
            self._reader_connections.append(None)

            thread = threading.Thread(
                target=self._reader_loop,
                args=(i,),
                name=f"pasyn-sqlite-reader-{i}",
                daemon=True,
            )
            state.thread = thread
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

        while not self._shutdown_event.is_set():
            task = None

            with self._writer_lock:
                if self._writer_queue:
                    task = self._writer_queue.popleft()

            if task is not None:
                self._execute_task(task, self._writer_connection)
            else:
                # Wait for new work or shutdown
                self._writer_event.wait(timeout=0.1)
                self._writer_event.clear()

        # Cleanup
        if self._writer_connection:
            self._writer_connection.close()

    def _reader_loop(self, reader_id: int) -> None:
        """Main loop for a reader thread with work stealing."""
        # Wait for writer to be ready (WAL mode set)
        self._writer_ready.wait(timeout=5.0)

        connection = self._create_reader_connection()
        self._reader_connections[reader_id] = connection
        my_state = self._readers[reader_id]

        while not self._shutdown_event.is_set():
            task = self._get_reader_task(reader_id, my_state)

            if task is not None:
                self._execute_task(task, connection)
            else:
                # Wait for new work or shutdown
                self._reader_event.wait(timeout=0.1)
                self._reader_event.clear()

        # Cleanup
        connection.close()

    def _get_reader_task(
        self, reader_id: int, my_state: ReaderState
    ) -> Task | None:
        """
        Get a task for a reader using work-stealing strategy.

        Priority:
        1. Local queue (fast path)
        2. Steal from other readers
        3. Shared queue (new work distribution)
        """
        # 1. Try local queue first (fast path)
        with my_state.lock:
            if my_state.local_queue:
                return my_state.local_queue.popleft()

        # 2. Try to steal from other readers
        for i, other_state in enumerate(self._readers):
            if i == reader_id:
                continue

            with other_state.lock:
                if other_state.local_queue:
                    # Steal from the back (opposite end from owner)
                    try:
                        return other_state.local_queue.pop()
                    except IndexError:
                        continue

        # 3. Try shared queue
        with self._shared_read_lock:
            if self._shared_read_queue:
                return self._shared_read_queue.popleft()

        return None

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

        with self._writer_lock:
            self._writer_queue.append(task)
        self._writer_event.set()

        return future

    def submit_read(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """
        Submit a read task to the reader pool.

        The task will be added to the shared queue and picked up by
        an available reader. Readers use work-stealing for load balancing.

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

        with self._shared_read_lock:
            self._shared_read_queue.append(task)
        self._reader_event.set()

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
        self._shutdown_event.set()

        # Wake up all threads
        self._writer_event.set()
        self._reader_event.set()

        if wait:
            if self._writer_thread:
                self._writer_thread.join(timeout=5.0)

            for state in self._readers:
                if state.thread:
                    state.thread.join(timeout=5.0)

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
