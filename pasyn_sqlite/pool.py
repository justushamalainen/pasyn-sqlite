"""
Lock-free thread pool with Single Writer Multiple Reader (SWMR) architecture.

Uses Python's GIL atomicity guarantees:
- deque.append() and deque.popleft() are atomic (single bytecode)
- queue.Queue provides thread-safe multi-consumer access
- ThreadPoolExecutor handles work-stealing for readers
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum, auto
from queue import Empty, Queue
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
    Lock-free thread pool for SQLite with SWMR architecture.

    - Single writer thread: Handles all write operations (INSERT, UPDATE, etc.)
    - Multiple reader threads: Handle SELECT queries with work-stealing

    Uses GIL atomicity for lock-free queue operations:
    - Write queue: collections.deque (SPSC - single consumer)
    - Read queue: queue.Queue (SPMC - multiple consumers with work-stealing)
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
        self._shutdown = threading.Event()

        # Write queue - deque for lock-free SPSC (GIL atomic append/popleft)
        self._write_queue: deque[Task | None] = deque()
        self._write_event = threading.Event()  # Signal writer when work available

        # Read queue - Queue for SPMC with built-in blocking
        self._read_queue: Queue[Task | None] = Queue()

        # Writer thread
        self._writer_thread: threading.Thread | None = None
        self._writer_connection: sqlite3.Connection | None = None
        self._writer_ready = threading.Event()

        # Reader pool - ThreadPoolExecutor handles work-stealing
        self._reader_executor: ThreadPoolExecutor | None = None
        self._reader_connections: list[sqlite3.Connection] = []
        self._reader_conn_lock = threading.Lock()  # Only for connection list init

        self._start_threads()

    def _start_threads(self) -> None:
        """Start writer thread and reader pool."""
        # Start single writer thread
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="pasyn-sqlite-writer",
            daemon=True,
        )
        self._writer_thread.start()

        # Start reader thread pool
        self._reader_executor = ThreadPoolExecutor(
            max_workers=self._num_readers,
            thread_name_prefix="pasyn-sqlite-reader",
        )

        # Submit reader workers to the pool
        for i in range(self._num_readers):
            self._reader_executor.submit(self._reader_loop, i)

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
        """
        Main loop for the single writer thread.

        Uses lock-free deque operations (GIL atomic).
        """
        self._writer_connection = self._create_writer_connection()
        self._writer_ready.set()

        while not self._shutdown.is_set():
            # Try to get task from deque (lock-free, GIL atomic)
            try:
                task = self._write_queue.popleft()
                if task is None:  # Shutdown signal
                    break
                self._execute_task(task, self._writer_connection)
            except IndexError:
                # Queue empty - wait for signal with timeout
                self._write_event.wait(timeout=0.01)
                self._write_event.clear()

        if self._writer_connection:
            self._writer_connection.close()

    def _reader_loop(self, reader_id: int) -> None:
        """
        Main loop for a reader thread.

        Uses Queue.get() for thread-safe work-stealing.
        """
        # Wait for writer to establish WAL mode
        self._writer_ready.wait(timeout=5.0)

        conn = self._create_reader_connection()
        with self._reader_conn_lock:
            self._reader_connections.append(conn)

        while not self._shutdown.is_set():
            try:
                # Queue.get with timeout - blocks until task available
                # Multiple readers compete for tasks (work-stealing)
                task = self._read_queue.get(timeout=0.01)
                if task is None:  # Shutdown signal
                    # Re-queue None for other readers
                    self._read_queue.put(None)
                    break
                self._execute_task(task, conn)
            except Empty:
                continue  # No task, check shutdown and retry

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
        """
        Submit a write task to the writer thread.

        Lock-free: uses deque.append() which is GIL atomic.
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

        # Lock-free append (GIL atomic)
        self._write_queue.append(task)
        self._write_event.set()  # Signal writer

        return future

    def submit_read(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """
        Submit a read task to the reader pool.

        Uses Queue.put() for thread-safe multi-consumer access.
        Readers compete for tasks (work-stealing via Queue).
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

        self._read_queue.put(task)
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
        """
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
        self._shutdown.set()

        # Send shutdown signals
        self._write_queue.append(None)
        self._write_event.set()
        self._read_queue.put(None)

        if wait:
            if self._writer_thread:
                self._writer_thread.join(timeout=5.0)

            if self._reader_executor:
                self._reader_executor.shutdown(wait=True, cancel_futures=False)

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
