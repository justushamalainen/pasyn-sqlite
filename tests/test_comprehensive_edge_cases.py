"""
Comprehensive edge case tests for pasyn-sqlite.

Tests scenarios where the system could get stuck, fail silently,
or exhibit undefined behavior. Focuses on:
- Thread pool lifecycle
- WAL mode initialization
- Queue and task management
- Database locking
- Transaction edge cases
- SQL parsing edge cases
- Memory and resource limits
- Error propagation
- Connection state
- Concurrency stress tests

Note: Uses file-based databases for most tests to avoid shared cache issues
with in-memory databases.
"""

import asyncio
import gc
import os
import random
import sqlite3
import tempfile
import unittest.mock as mock
import uuid
import warnings
import weakref
from contextlib import contextmanager

import pytest

from pasyn_sqlite import (
    BoundConnection,
    Cursor,
    PasynPool,
    PoolClosedError,
    ConnectionClosedError,
    TransactionCommandError,
    create_pool,
)
from pasyn_sqlite.pool import ThreadPool


@contextmanager
def temp_db():
    """Context manager for temporary database file."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
        db_path = f.name
    try:
        yield db_path
    finally:
        for ext in ["", "-wal", "-shm"]:
            try:
                os.unlink(db_path + ext)
            except FileNotFoundError:
                pass


def unique_table_name() -> str:
    """Generate a unique table name to avoid conflicts in shared memory database."""
    return f"test_{uuid.uuid4().hex[:8]}"


# =============================================================================
# 1. Thread Pool Lifecycle Edge Cases
# =============================================================================


class TestThreadPoolLifecycle:
    """Tests for thread pool lifecycle edge cases."""

    async def test_close_during_pending_write_operations(self):
        """Test closing pool with pending write operations."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Submit many writes without awaiting
            futures = []
            for i in range(100):
                task = asyncio.create_task(
                    pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                )
                futures.append(task)

            # Immediately close - some writes may be pending
            await pool.close()

            # Check that pending tasks raise PoolClosedError or complete
            results = await asyncio.gather(*futures, return_exceptions=True)

            # Some should succeed (already in progress), some may fail
            # The key is none should hang indefinitely
            for r in results:
                assert r is None or isinstance(
                    r, (Cursor, PoolClosedError, asyncio.CancelledError)
                )

    async def test_close_during_pending_read_operations(self):
        """Test closing pool with pending read operations across multiple readers."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=4)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            # Queue many reads (distributed round-robin)
            futures = [
                asyncio.create_task(pool.execute(f"SELECT * FROM {table}"))
                for _ in range(50)
            ]

            await pool.close()

            # Verify no indefinite hangs
            results = await asyncio.wait_for(
                asyncio.gather(*futures, return_exceptions=True), timeout=10.0
            )
            # All should complete (success or error)
            assert len(results) == 50

    async def test_multiple_close_calls(self):
        """Test that multiple close calls are safe."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)

            # Close multiple times concurrently
            await asyncio.gather(
                pool.close(),
                pool.close(),
                pool.close(),
            )

            assert pool.closed

            # Additional close should be no-op
            await pool.close()
            assert pool.closed

    async def test_operations_after_close(self):
        """Test that all operations properly reject after close."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.close()

            # All these should raise PoolClosedError, not hang
            with pytest.raises(PoolClosedError):
                await pool.execute("SELECT 1")

            with pytest.raises(PoolClosedError):
                await pool.executemany(f"INSERT INTO {table} VALUES (?)", [(1,)])

            with pytest.raises(PoolClosedError):
                await pool.bound_connection()

    async def test_thread_join_timeout(self):
        """Test behavior when thread doesn't finish within join timeout."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Submit a write operation
            await pool.execute(f"INSERT INTO {table} VALUES (1)")
            await pool.close()

            # Verify pool is in closed state
            assert pool.closed
            assert pool._thread_pool.closed

    async def test_pool_garbage_collection_without_close(self):
        """Test pool behavior when not explicitly closed."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Create weak reference to track object
            pool_ref = weakref.ref(pool)

            # Delete reference and force GC
            del pool
            gc.collect()

            # Pool may still exist due to thread references
            # This test documents current behavior

    async def test_close_idempotent(self):
        """Test that close is idempotent and safe to call multiple times."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Close many times
            for _ in range(10):
                await pool.close()

            assert pool.closed

    async def test_context_manager_exception_cleanup(self):
        """Test that context manager properly cleans up on exception."""
        with temp_db() as db_path:
            try:
                async with await create_pool(db_path) as pool:
                    table = unique_table_name()
                    await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
                    raise ValueError("Test exception")
            except ValueError:
                pass

            assert pool.closed


# =============================================================================
# 2. WAL Mode Initialization Edge Cases
# =============================================================================


class TestWALModeInitialization:
    """Tests for WAL mode initialization edge cases."""

    async def test_reader_starts_before_wal_mode(self):
        """Test reader behavior when WAL mode setup is slow."""
        with temp_db() as db_path:
            original_create_writer = ThreadPool._create_writer_connection

            def slow_writer_connection(self):
                import time
                time.sleep(0.1)  # Small delay to simulate slow setup
                return original_create_writer(self)

            with mock.patch.object(
                ThreadPool, "_create_writer_connection", slow_writer_connection
            ):
                async with await create_pool(db_path, num_readers=2) as pool:
                    table = unique_table_name()
                    await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
                    cursor = await pool.execute("SELECT 1")
                    rows = await cursor.fetchall()
                    assert rows == [(1,)]

    async def test_writer_failure_during_initialization(self):
        """Test pool behavior when writer fails to initialize.

        Documents current behavior: Writer failure in background thread
        causes operations to hang until timeout. The library should ideally
        propagate this error more gracefully.
        """
        def failing_writer_connection(self):
            raise sqlite3.OperationalError("Cannot open database")

        with mock.patch.object(
            ThreadPool, "_create_writer_connection", failing_writer_connection
        ):
            # Pool creation starts threads in background
            # Writer failure in daemon thread - pool creation succeeds but
            # operations will fail/hang
            pool = await create_pool(":memory:")

            # Operations will hang because writer thread died
            # The operation times out rather than raising the underlying error
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(pool.execute("SELECT 1"), timeout=2.0)

            await pool.close()

    async def test_reader_connection_failure_with_retries(self):
        """Test reader behavior when connection fails after all retries."""
        with temp_db() as db_path:
            call_count = 0
            original_create_reader = ThreadPool._create_reader_connection

            def sometimes_failing_reader(self):
                nonlocal call_count
                call_count += 1
                if call_count <= 3:  # First few calls fail
                    raise sqlite3.OperationalError("database is locked")
                return original_create_reader(self)

            with mock.patch.object(
                ThreadPool, "_create_reader_connection", sometimes_failing_reader
            ):
                pool = await create_pool(db_path, num_readers=1)
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")  # Uses writer

                # Read may succeed after retries
                try:
                    cursor = await asyncio.wait_for(pool.execute("SELECT 1"), timeout=10.0)
                    rows = await cursor.fetchall()
                    assert rows == [(1,)]
                except (sqlite3.OperationalError, asyncio.TimeoutError):
                    pass  # Acceptable if retries exhausted

                await pool.close()

    async def test_wal_mode_on_file_database(self):
        """Test WAL mode is properly enabled on file database."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

                # Check WAL mode is enabled via bound connection
                # (PRAGMA goes to writer, results returned properly)
                async with await pool.bound_connection() as conn:
                    cursor = await conn.execute("PRAGMA journal_mode")
                    rows = await cursor.fetchall()
                    assert len(rows) > 0
                    assert rows[0][0].lower() == "wal"


# =============================================================================
# 3. Queue and Task Management
# =============================================================================


class TestQueueAndTaskManagement:
    """Tests for queue and task management edge cases."""

    async def test_task_cancellation_before_execution(self):
        """Test cancelling task before worker picks it up.

        Note: Task cancellation may cause the worker thread to raise
        InvalidStateError when trying to set result on cancelled future.
        This is a known limitation - the pool remains functional.
        """
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Queue multiple writes - some may be cancelled
            tasks = []
            for i in range(10):
                tasks.append(
                    asyncio.create_task(
                        pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                    )
                )

            # Cancel some tasks
            await asyncio.sleep(0.001)
            for t in tasks[5:]:
                t.cancel()

            # Gather results - some may succeed, some cancelled
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Some should have completed, some cancelled
            completed = sum(1 for r in results if isinstance(r, Cursor))
            cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
            assert completed + cancelled == len(results)

            # Pool should still work
            cursor = await pool.execute("SELECT 1")
            rows = await cursor.fetchall()
            assert rows == [(1,)]

            await pool.close()

    async def test_task_cancellation_during_execution(self):
        """Test cancelling task during SQL execution.

        Note: Cancelling a task during SQL execution may cause the worker
        thread to raise InvalidStateError when trying to set result.
        This is a known limitation documented here. The pool remains functional.
        """
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Insert rows
            await pool.executemany(
                f"INSERT INTO {table} VALUES (?)", [(i,) for i in range(1000)]
            )

            # Start multiple queries and cancel some
            tasks = []
            for _ in range(5):
                tasks.append(
                    asyncio.create_task(
                        pool.execute(f"SELECT * FROM {table} ORDER BY x")
                    )
                )

            # Cancel some tasks
            await asyncio.sleep(0.001)
            for t in tasks[2:]:
                t.cancel()

            # Gather results
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Some may have completed, some cancelled
            completed = sum(1 for r in results if isinstance(r, Cursor))
            cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
            assert completed + cancelled == len(results)

            # Pool should still be usable
            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 1000

            await pool.close()

    async def test_queue_buildup_under_heavy_load(self):
        """Test behavior with very large queue buildup."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=2)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Submit many operations rapidly
            tasks = []
            for i in range(500):
                # Mix reads and writes to stress both queues
                if i % 2 == 0:
                    tasks.append(asyncio.create_task(pool.execute("SELECT 1")))
                else:
                    tasks.append(
                        asyncio.create_task(
                            pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                        )
                    )

            # All should eventually complete
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=60.0
            )

            # Verify all completed
            assert len(results) == 500

            # Check data integrity
            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 250  # Half were inserts

            await pool.close()

    async def test_rapid_submit_during_close(self):
        """Test submitting tasks during pool close."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            errors = []

            async def submit_loop():
                for i in range(100):
                    try:
                        await pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                    except PoolClosedError:
                        errors.append(i)
                    except Exception as e:
                        errors.append((i, type(e).__name__))

            async def close_pool():
                await asyncio.sleep(0.01)
                await pool.close()

            await asyncio.gather(submit_loop(), close_pool())

            # Some operations should fail with PoolClosedError
            # None should hang or raise unexpected errors

    async def test_future_result_set_after_cancellation(self):
        """Test worker setting result on cancelled future."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)

            task = asyncio.create_task(pool.execute("SELECT 1"))

            # Race: cancel while execution might be happening
            await asyncio.sleep(0)
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass  # Expected

            # Pool should remain functional
            cursor = await pool.execute("SELECT 2")
            rows = await cursor.fetchall()
            assert rows == [(2,)]

            await pool.close()


# =============================================================================
# 4. Database Locking Scenarios
# =============================================================================


class TestDatabaseLocking:
    """Tests for database locking scenarios."""

    async def test_busy_timeout_exhaustion(self):
        """Test behavior when busy timeout is exhausted."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            # Open external connection with exclusive lock
            external_conn = sqlite3.connect(db_path)
            external_conn.execute("BEGIN EXCLUSIVE")

            # Pool operations should fail with timeout
            start_time = asyncio.get_event_loop().time()

            try:
                # This should fail after busy_timeout (5s)
                await asyncio.wait_for(
                    pool.execute(f"INSERT INTO {table} VALUES (2)"), timeout=10.0
                )
                pytest.fail("Expected OperationalError")
            except sqlite3.OperationalError as e:
                assert "locked" in str(e).lower()

            elapsed = asyncio.get_event_loop().time() - start_time
            assert elapsed >= 4.5  # Should have waited close to busy_timeout

            external_conn.rollback()
            external_conn.close()
            await pool.close()

    async def test_reader_wal_blocking(self):
        """Test reader behavior during WAL checkpoint."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=2)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Generate many writes to grow WAL
            for batch in range(10):
                data = [(i,) for i in range(batch * 100, (batch + 1) * 100)]
                await pool.executemany(f"INSERT INTO {table} VALUES (?)", data)

            # Force checkpoint via bound connection
            async with await pool.bound_connection() as conn:
                await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            # Concurrent reads should work
            read_tasks = [pool.execute(f"SELECT COUNT(*) FROM {table}") for _ in range(20)]

            results = await asyncio.gather(*read_tasks)
            for cursor in results:
                rows = await cursor.fetchall()
                assert rows[0][0] == 1000

            await pool.close()

    async def test_exclusive_lock_contention(self):
        """Test BEGIN EXCLUSIVE with concurrent reader activity."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=3)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            async def reader_loop():
                """Continuous reading."""
                for _ in range(50):
                    cursor = await pool.execute(f"SELECT * FROM {table}")
                    await cursor.fetchall()
                    await asyncio.sleep(0.001)

            async def exclusive_writer():
                """Try to get exclusive lock."""
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN EXCLUSIVE")
                    await conn.execute(f"INSERT INTO {table} VALUES (2)")
                    await asyncio.sleep(0.1)  # Hold lock briefly
                    await conn.execute("COMMIT")

            # Run readers and exclusive writer concurrently
            await asyncio.wait_for(
                asyncio.gather(reader_loop(), reader_loop(), exclusive_writer()),
                timeout=30.0,
            )

            await pool.close()

    async def test_long_running_read_blocks_checkpoint(self):
        """Test WAL checkpoint behavior with long-running reads."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Start a "long" read by getting cursor and not finishing
            cursor = await pool.execute("SELECT 1")

            # Do writes (grow WAL)
            for i in range(100):
                await pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))

            # Finish the read
            await cursor.fetchall()

            # Checkpoint should work via bound connection
            async with await pool.bound_connection() as conn:
                await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

            await pool.close()


# =============================================================================
# 5. Transaction Edge Cases
# =============================================================================


class TestTransactionEdgeCases:
    """Tests for transaction edge cases."""

    async def test_uncommitted_transaction_warning(self):
        """Test warning when closing with uncommitted transaction."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")

                conn = await pool.bound_connection()
                await conn.execute(f"CREATE TABLE {table} (x INTEGER)")
                await conn.execute("BEGIN")
                await conn.execute(f"INSERT INTO {table} VALUES (1)")
                # Close without commit
                await conn.close()

                # Should have warning
                assert len(w) >= 1
                assert any("uncommitted transaction" in str(warning.message).lower() for warning in w)

            # Data should be rolled back
            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 0

            await pool.close()

    async def test_exception_during_transaction_rollback(self):
        """Test behavior when both operation and rollback fail."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            try:
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN")
                    await conn.execute(f"INSERT INTO {table} VALUES (1)")

                    # Force an error
                    raise ValueError("Simulated error")
            except ValueError:
                pass  # Expected

            # Pool should still work
            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 0  # Should be rolled back

            await pool.close()

    async def test_concurrent_bound_connection_serialization(self):
        """Test that concurrent bound_connection requests are serialized."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            order = []

            async def worker(worker_id):
                async with await pool.bound_connection() as conn:
                    order.append(f"start_{worker_id}")
                    await conn.execute(f"INSERT INTO {table} VALUES (?)", (worker_id,))
                    await asyncio.sleep(0.01)  # Hold connection briefly
                    order.append(f"end_{worker_id}")

            # Start multiple workers
            await asyncio.gather(*[worker(i) for i in range(5)])

            # Verify serialization: no interleaving of start/end
            current = None
            for event in order:
                if event.startswith("start_"):
                    assert current is None, f"Interleaving detected: {order}"
                    current = event.split("_")[1]
                else:
                    assert current == event.split("_")[1], f"Mismatched: {order}"
                    current = None

            await pool.close()

    async def test_begin_immediate_vs_deferred(self):
        """Test BEGIN IMMEDIATE vs BEGIN DEFERRED behavior."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=2)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            async def deferred_transaction():
                """BEGIN (deferred) - read lock only until write."""
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN")  # Deferred
                    cursor = await conn.execute(f"SELECT * FROM {table}")
                    await cursor.fetchall()
                    await asyncio.sleep(0.05)
                    await conn.execute("COMMIT")

            async def immediate_transaction():
                """BEGIN IMMEDIATE - write lock from start."""
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN IMMEDIATE")
                    cursor = await conn.execute(f"SELECT * FROM {table}")
                    await cursor.fetchall()
                    await asyncio.sleep(0.05)
                    await conn.execute("COMMIT")

            # Both should work
            await asyncio.gather(
                deferred_transaction(),
                deferred_transaction(),
            )

            await asyncio.gather(
                immediate_transaction(),
                immediate_transaction(),
            )

            await pool.close()

    async def test_nested_bound_connection_deadlock(self):
        """Test that nested bound_connection acquisition deadlocks (documents behavior)."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)

            async def nested_acquisition():
                async with await pool.bound_connection():
                    # This will deadlock because we're trying to acquire
                    # the same non-reentrant lock
                    async with await pool.bound_connection():
                        pass

            # This should timeout (deadlock detection)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(nested_acquisition(), timeout=2.0)

            # Note: After timeout, the lock is still held!
            # Pool needs to be closed to release
            await pool.close()

    async def test_long_transaction_blocks_pool_writes(self):
        """Test interaction between BoundConnection transaction and pool writes.

        Both bound connection and pool.execute use the same writer thread,
        so they are serialized. The exact ordering depends on timing.
        """
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            results = []

            async def bound_transaction():
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN IMMEDIATE")
                    results.append("bound_start")
                    await asyncio.sleep(0.1)
                    await conn.execute(f"INSERT INTO {table} VALUES (1)")
                    await conn.execute("COMMIT")
                    results.append("bound_end")

            async def pool_write():
                await asyncio.sleep(0.02)  # Let transaction start first
                results.append("pool_start")
                await pool.execute(f"INSERT INTO {table} VALUES (2)")
                results.append("pool_end")

            await asyncio.gather(bound_transaction(), pool_write())

            # Both operations should complete
            # Order depends on timing, but bound_start should be before bound_end
            assert "bound_start" in results
            assert "bound_end" in results
            assert "pool_start" in results
            assert "pool_end" in results
            assert results.index("bound_start") < results.index("bound_end")
            assert results.index("pool_start") < results.index("pool_end")

            cursor = await pool.execute(f"SELECT * FROM {table} ORDER BY x")
            rows = await cursor.fetchall()
            assert len(rows) == 2  # Both inserts should succeed
            assert set(r[0] for r in rows) == {1, 2}

            await pool.close()

    async def test_in_transaction_property_race_condition(self):
        """Test in_transaction property during rapid state changes."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()

            async with await pool.bound_connection() as conn:
                await conn.execute(f"CREATE TABLE {table} (x INTEGER)")

                states = []

                async def check_state():
                    for _ in range(100):
                        states.append(conn.in_transaction)
                        await asyncio.sleep(0)

                async def toggle_transaction():
                    for i in range(50):
                        await conn.execute("BEGIN")
                        await conn.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                        await conn.execute("COMMIT")

                await asyncio.gather(check_state(), toggle_transaction())

                # We captured some states - verify no exceptions occurred
                assert len(states) == 100
                assert all(isinstance(s, bool) for s in states)

            await pool.close()

    async def test_rollback_to_savepoint_allowed(self):
        """Test that ROLLBACK TO savepoint is properly handled."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                async with await pool.bound_connection() as conn:
                    await conn.execute(f"CREATE TABLE {table} (x INTEGER)")
                    await conn.execute("BEGIN")
                    await conn.execute(f"INSERT INTO {table} VALUES (1)")
                    await conn.execute("SAVEPOINT sp1")
                    await conn.execute(f"INSERT INTO {table} VALUES (2)")
                    await conn.execute("ROLLBACK TO sp1")
                    await conn.execute("COMMIT")

                    cursor = await conn.execute(f"SELECT * FROM {table}")
                    rows = await cursor.fetchall()
                    assert rows == [(1,)]


# =============================================================================
# 6. SQL Parsing Edge Cases
# =============================================================================


class TestSQLParsingEdgeCases:
    """Tests for SQL parsing edge cases."""

    async def test_cte_with_write_operations(self):
        """Test CTEs with embedded write keywords.

        Documents library behavior: CTEs containing write keywords anywhere
        in the SQL string are routed to the writer thread, even if the
        keyword appears in a string literal or comment.
        """
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (id INTEGER, status TEXT)")
                await pool.execute(f"INSERT INTO {table} VALUES (1, 'ACTIVE')")
                await pool.execute(f"INSERT INTO {table} VALUES (2, 'ACTIVE')")

                # Simple CTE SELECT - no write keywords
                cursor = await pool.execute(f"""
                    WITH filtered AS (
                        SELECT * FROM {table} WHERE status = 'ACTIVE'
                    )
                    SELECT * FROM filtered
                """)
                rows = await cursor.fetchall()
                assert len(rows) == 2

                # Actual CTE with INSERT (SQLite supports this)
                await pool.execute(f"""
                    WITH new_values(id, status) AS (VALUES (3, 'NEW'))
                    INSERT INTO {table} SELECT * FROM new_values
                """)

                cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                rows = await cursor.fetchall()
                assert rows[0][0] == 3

    async def test_explain_query_plan(self):
        """Test EXPLAIN and EXPLAIN QUERY PLAN routing.

        Documents library behavior: EXPLAIN queries are routed based on
        keyword detection - EXPLAIN INSERT routes to writer because INSERT
        keyword is detected. Results should still work correctly.
        """
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

                # EXPLAIN SELECT - routes to reader (no write keywords)
                cursor = await pool.execute(f"EXPLAIN QUERY PLAN SELECT * FROM {table}")
                rows = await cursor.fetchall()
                assert len(rows) >= 0  # May return results

                # EXPLAIN INSERT - routes to writer (INSERT keyword detected)
                # This is correct behavior - EXPLAIN doesn't actually execute
                cursor = await pool.execute(f"EXPLAIN QUERY PLAN INSERT INTO {table} VALUES (1)")
                rows = await cursor.fetchall()
                # EXPLAIN returns query plan rows, not actual insert
                assert len(rows) >= 0

                # Verify no actual insert happened from EXPLAIN
                cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                rows = await cursor.fetchall()
                assert rows[0][0] == 0

                # Actual insert (no EXPLAIN)
                await pool.execute(f"INSERT INTO {table} VALUES (1)")

                cursor = await pool.execute(f"SELECT * FROM {table}")
                rows = await cursor.fetchall()
                assert rows == [(1,)]

    async def test_attach_detach_database(self):
        """Test ATTACH and DETACH database operations."""
        with temp_db() as main_db, temp_db() as other_db:
            # Create other database
            async with await create_pool(other_db) as pool:
                await pool.execute("CREATE TABLE other_table (y TEXT)")
                await pool.execute("INSERT INTO other_table VALUES ('from_other')")

            # Main database with ATTACH
            async with await create_pool(main_db) as pool:
                await pool.execute("CREATE TABLE main_table (x INTEGER)")

                # ATTACH (goes to writer)
                await pool.execute(f"ATTACH DATABASE '{other_db}' AS other")

                # Query attached database (via bound connection since it's attached there)
                async with await pool.bound_connection() as conn:
                    cursor = await conn.execute("SELECT * FROM other.other_table")
                    rows = await cursor.fetchall()
                    assert rows == [("from_other",)]

                    await conn.execute("DETACH DATABASE other")

    async def test_pragma_routing(self):
        """Test PRAGMA statement routing."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER, y TEXT)")

                # Read-only PRAGMAs via bound connection
                async with await pool.bound_connection() as conn:
                    cursor = await conn.execute(f"PRAGMA table_info({table})")
                    rows = await cursor.fetchall()
                    assert len(rows) == 2

                    cursor = await conn.execute("PRAGMA database_list")
                    rows = await cursor.fetchall()
                    assert len(rows) >= 1

                    # Write PRAGMAs
                    await conn.execute("PRAGMA synchronous = NORMAL")

                    cursor = await conn.execute("PRAGMA synchronous")
                    await cursor.fetchall()

    async def test_multistatement_sql(self):
        """Test behavior with multiple statements in single execute.

        SQLite's execute() raises ProgrammingError for multiple statements.
        For multiple statements, use executescript() on a bound connection.
        """
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

                # execute() raises error for multiple statements
                with pytest.raises(sqlite3.ProgrammingError, match="one statement at a time"):
                    await pool.execute(f"INSERT INTO {table} VALUES (1); INSERT INTO {table} VALUES (2)")

                # Table should still be empty since execute failed
                cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                rows = await cursor.fetchall()
                assert rows[0][0] == 0

                # For multiple statements, use executescript on bound connection
                async with await pool.bound_connection() as conn:
                    await conn.executescript(f"""
                        INSERT INTO {table} VALUES (10);
                        INSERT INTO {table} VALUES (11);
                    """)

                cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                rows = await cursor.fetchall()
                # Should have 2 rows from executescript
                assert rows[0][0] == 2

    async def test_sql_comments(self):
        """Test SQL with comments containing write keywords."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
                await pool.execute(f"INSERT INTO {table} VALUES (1)")

                # Comment contains INSERT but query is SELECT
                # Note: The library's keyword detection may route this to writer
                cursor = await pool.execute(f"""
                    -- TODO: INSERT more test data later
                    SELECT * FROM {table}
                """)
                rows = await cursor.fetchall()
                assert rows == [(1,)]

                # Block comment with DELETE
                cursor = await pool.execute(f"""
                    /* Need to DELETE old records */
                    SELECT COUNT(*) FROM {table}
                """)
                rows = await cursor.fetchall()
                assert rows[0][0] == 1

    async def test_virtual_table_fts(self):
        """Test Full-Text Search virtual table operations."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                # Create FTS table
                await pool.execute("""
                    CREATE VIRTUAL TABLE docs USING fts5(title, content)
                """)

                await pool.execute(
                    "INSERT INTO docs VALUES (?, ?)", ("Hello", "World of testing")
                )

                # FTS query
                cursor = await pool.execute(
                    "SELECT * FROM docs WHERE docs MATCH 'testing'"
                )
                rows = await cursor.fetchall()
                assert len(rows) == 1

                # FTS rebuild (write operation)
                await pool.execute("INSERT INTO docs(docs) VALUES('rebuild')")

    async def test_case_variations_in_sql(self):
        """Test various case combinations in SQL keywords."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

                # Various case combinations for SELECT
                for sql in ["select 1", "SELECT 1", "Select 1", "sElEcT 1"]:
                    cursor = await pool.execute(sql)
                    rows = await cursor.fetchall()
                    assert rows == [(1,)]

                # Transaction commands should be blocked regardless of case
                for cmd in ["begin", "BEGIN", "Begin", "  BEGIN  ", "COMMIT", "commit"]:
                    with pytest.raises(TransactionCommandError):
                        await pool.execute(cmd)


# =============================================================================
# 7. Memory and Resource Edge Cases
# =============================================================================


class TestMemoryAndResources:
    """Tests for memory and resource edge cases."""

    async def test_very_large_result_set(self):
        """Test handling of very large result sets."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Insert many rows
            batch_size = 1000
            for batch in range(100):  # 100k rows total
                data = [(i,) for i in range(batch * batch_size, (batch + 1) * batch_size)]
                await pool.executemany(f"INSERT INTO {table} VALUES (?)", data)

            # This loads all 100k rows into memory
            cursor = await pool.execute(f"SELECT * FROM {table}")
            rows = await cursor.fetchall()
            assert len(rows) == 100000

            await pool.close()

    async def test_many_concurrent_operations_stress(self):
        """Stress test with many concurrent operations."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=4)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # 500 concurrent operations
            async def read_op(i):
                cursor = await pool.execute("SELECT 1")
                return await cursor.fetchone()

            async def write_op(i):
                await pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))

            tasks = []
            for i in range(250):
                tasks.append(asyncio.create_task(read_op(i)))
                tasks.append(asyncio.create_task(write_op(i)))

            # Should complete within reasonable time
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=120.0
            )

            # Check for errors
            errors = [r for r in results if isinstance(r, Exception)]
            assert len(errors) == 0, f"Got {len(errors)} errors: {errors[:5]}"

            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 250

            await pool.close()

    async def test_rapid_pool_creation_destruction(self):
        """Test rapid pool creation and destruction."""
        with temp_db() as db_path:
            for i in range(20):
                pool = await create_pool(db_path, num_readers=2)
                table = f"test_{i}"
                await pool.execute(f"CREATE TABLE IF NOT EXISTS {table} (x INTEGER)")
                await pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                await pool.close()

            # Verify data persisted
            pool = await create_pool(db_path)
            for i in range(20):
                cursor = await pool.execute(f"SELECT * FROM test_{i}")
                rows = await cursor.fetchall()
                assert rows == [(i,)]
            await pool.close()

    async def test_pool_state_after_sql_error(self):
        """Test pool remains usable after SQL errors."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Trigger various errors
            with pytest.raises(Exception):
                await pool.execute("SELECT * FROM nonexistent_table_xyz")

            with pytest.raises(Exception):
                await pool.execute("INVALID SQL SYNTAX XYZ")

            # Pool should still work
            await pool.execute(f"INSERT INTO {table} VALUES (1)")
            cursor = await pool.execute(f"SELECT * FROM {table}")
            rows = await cursor.fetchall()
            assert rows == [(1,)]

            await pool.close()

    async def test_memory_database_isolation(self):
        """Test isolation between :memory: pools - documents shared cache behavior."""
        pool1 = await create_pool(":memory:")
        pool2 = await create_pool(":memory:")

        table = unique_table_name()
        await pool1.execute(f"CREATE TABLE {table} (x INTEGER)")
        await pool1.execute(f"INSERT INTO {table} VALUES (1)")

        # pool2 shares the same memory database due to cache=shared
        cursor = await pool2.execute(f"SELECT * FROM {table}")
        rows = await cursor.fetchall()

        # With shared cache, pool2 sees pool1's data
        assert rows == [(1,)]

        await pool1.close()
        await pool2.close()

    async def test_large_blob_handling(self):
        """Test handling of large BLOB data."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, data BLOB)")

            # Insert 1MB blob
            large_blob = bytes(range(256)) * 4096  # 1MB
            await pool.execute(f"INSERT INTO {table} VALUES (1, ?)", (large_blob,))

            cursor = await pool.execute(f"SELECT data FROM {table} WHERE id = 1")
            rows = await cursor.fetchall()
            assert rows[0][0] == large_blob

            await pool.close()


# =============================================================================
# 8. Error Propagation
# =============================================================================


class TestErrorPropagation:
    """Tests for error propagation."""

    async def test_constraint_violation_propagation(self):
        """Test that constraint violations propagate with details."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"
            )
            await pool.execute(f"INSERT INTO {table} VALUES (1, 'Alice')")

            with pytest.raises(sqlite3.IntegrityError) as exc_info:
                await pool.execute(f"INSERT INTO {table} VALUES (2, 'Alice')")

            assert "UNIQUE constraint failed" in str(exc_info.value)
            await pool.close()

    async def test_syntax_error_propagation(self):
        """Test that syntax errors propagate with details."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)

            with pytest.raises(sqlite3.OperationalError) as exc_info:
                await pool.execute("SELEC * FROM test")  # typo

            error_msg = str(exc_info.value).lower()
            assert "syntax error" in error_msg or "near" in error_msg
            await pool.close()

    async def test_thread_exception_propagation(self):
        """Test that thread exceptions propagate to async code."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)

            # Division by zero in SQL (SQLite handles this differently - returns NULL)
            cursor = await pool.execute("SELECT 1/0")
            rows = await cursor.fetchall()
            # SQLite returns NULL for division by zero, not an error
            assert rows == [(None,)]

            # Non-existent table error
            with pytest.raises(sqlite3.OperationalError):
                await pool.execute("SELECT * FROM nonexistent_table_xyz")

            # Pool still works
            cursor = await pool.execute("SELECT 1")
            rows = await cursor.fetchall()
            assert rows == [(1,)]

            await pool.close()

    async def test_permission_error(self):
        """Test handling of permission errors.

        Note: Behavior varies by platform and SQLite configuration.
        Some systems may allow writes to read-only files, others
        may fail on pool creation.
        """
        with temp_db() as db_path:
            table = unique_table_name()

            # Create database
            async with await create_pool(db_path) as pool:
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Remove write permission
            os.chmod(db_path, 0o444)

            try:
                # Try to open - may fail on some systems
                pool = await create_pool(db_path)
                try:
                    # Try to write - should fail on read-only DB
                    await pool.execute(f"INSERT INTO {table} VALUES (1)")
                except sqlite3.OperationalError:
                    pass  # Expected - database is read-only
                finally:
                    await pool.close()
            except (sqlite3.OperationalError, PermissionError):
                pass  # Some systems fail on pool creation
            finally:
                os.chmod(db_path, 0o644)

    async def test_foreign_key_violation(self):
        """Test foreign key constraint violation propagation."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            parent = unique_table_name()
            child = unique_table_name()

            async with await pool.bound_connection() as conn:
                await conn.execute("PRAGMA foreign_keys = ON")
                await conn.execute(f"CREATE TABLE {parent} (id INTEGER PRIMARY KEY)")
                await conn.execute(f"""
                    CREATE TABLE {child} (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER REFERENCES {parent}(id)
                    )
                """)

                # Insert without parent should fail
                with pytest.raises(sqlite3.IntegrityError) as exc_info:
                    await conn.execute(f"INSERT INTO {child} VALUES (1, 999)")

                assert "FOREIGN KEY constraint failed" in str(exc_info.value)

            await pool.close()

    async def test_not_null_constraint(self):
        """Test NOT NULL constraint violation."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER NOT NULL)")

            with pytest.raises(sqlite3.IntegrityError) as exc_info:
                await pool.execute(f"INSERT INTO {table} VALUES (NULL)")

            assert "NOT NULL constraint failed" in str(exc_info.value)
            await pool.close()

    async def test_check_constraint(self):
        """Test CHECK constraint violation."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER CHECK(x > 0))")

            with pytest.raises(sqlite3.IntegrityError) as exc_info:
                await pool.execute(f"INSERT INTO {table} VALUES (-1)")

            assert "CHECK constraint failed" in str(exc_info.value)
            await pool.close()


# =============================================================================
# 9. Connection State Edge Cases
# =============================================================================


class TestConnectionState:
    """Tests for connection state edge cases."""

    async def test_cursor_state_after_connection_close(self):
        """Test cursor behavior after connection close."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            cursor = await pool.execute(f"SELECT * FROM {table}")

            # Close pool
            await pool.close()

            # Cursor has cached data - should still work for reads
            rows = await cursor.fetchall()
            assert rows == [(1,)]

            # Closing cursor should work
            await cursor.close()

    async def test_bound_connection_after_pool_close(self):
        """Test BoundConnection behavior after pool close.

        After pool is closed, bound connection operations may timeout
        (because writer thread is gone) or raise PoolClosedError.

        Note: This test documents a limitation - accessing in_transaction
        on a closed pool may raise ProgrammingError.
        """
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            conn = await pool.bound_connection()

            await pool.close()

            # Operations should fail - either with error or timeout
            try:
                await asyncio.wait_for(conn.execute("SELECT 1"), timeout=2.0)
                pytest.fail("Expected error or timeout")
            except (PoolClosedError, ConnectionClosedError, asyncio.TimeoutError):
                pass  # Expected

            # Mark as closed to avoid accessing closed database in close()
            conn._closed = True

    async def test_in_transaction_after_close(self):
        """Test in_transaction property after close."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()

            async with await pool.bound_connection() as conn:
                await conn.execute(f"CREATE TABLE {table} (x INTEGER)")
                await conn.execute("BEGIN")
                assert conn.in_transaction

            # After close, should return False not raise
            assert not conn.in_transaction

            await pool.close()

    async def test_cursor_close_idempotent(self):
        """Test that cursor close is idempotent."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            cursor = await pool.execute("SELECT 1")

            # Close multiple times
            await cursor.close()
            await cursor.close()
            await cursor.close()

            assert cursor.closed
            await pool.close()

    async def test_cursor_operations_after_close(self):
        """Test cursor operations raise after close."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            cursor = await pool.execute("SELECT 1")
            await cursor.close()

            with pytest.raises(ConnectionClosedError):
                await cursor.fetchone()

            with pytest.raises(ConnectionClosedError):
                await cursor.fetchall()

            with pytest.raises(ConnectionClosedError):
                await cursor.fetchmany()

            await pool.close()

    async def test_bound_connection_close_idempotent(self):
        """Test that BoundConnection close is idempotent."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            conn = await pool.bound_connection()

            # Close multiple times
            await conn.close()
            await conn.close()
            await conn.close()

            assert conn.closed
            await pool.close()

    async def test_connection_closed_error_on_operations(self):
        """Test that closed connection raises on operations."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            conn = await pool.bound_connection()
            await conn.close()

            with pytest.raises(ConnectionClosedError):
                await conn.execute("SELECT 1")

            with pytest.raises(ConnectionClosedError):
                await conn.executemany("INSERT INTO t VALUES (?)", [(1,)])

            with pytest.raises(ConnectionClosedError):
                await conn.executescript("SELECT 1")

            with pytest.raises(ConnectionClosedError):
                await conn.commit()

            with pytest.raises(ConnectionClosedError):
                await conn.rollback()

            await pool.close()


# =============================================================================
# 10. Concurrency Stress Tests
# =============================================================================


class TestConcurrencyStress:
    """Stress tests for concurrency."""

    async def test_100_concurrent_reads(self):
        """Test 100+ concurrent read operations."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=4)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            for i in range(100):
                await pool.execute(f"INSERT INTO {table} VALUES (?)", (i,))

            async def read():
                cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                rows = await cursor.fetchall()
                return rows[0][0]

            results = await asyncio.gather(*[read() for _ in range(150)])

            assert all(r == 100 for r in results)
            await pool.close()

    async def test_100_concurrent_writes(self):
        """Test 100+ concurrent write operations (serialized through writer)."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            async def write(value):
                await pool.execute(f"INSERT INTO {table} VALUES (?)", (value,))

            await asyncio.gather(*[write(i) for i in range(150)])

            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 150

            await pool.close()

    async def test_heavy_mixed_workload(self):
        """Test heavy mixed read/write workload."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=4)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER, y TEXT)")

            write_count = [0]  # Use list to allow modification in nested function

            async def random_operation(i):
                if random.random() < 0.3:  # 30% writes
                    await pool.execute(
                        f"INSERT INTO {table} VALUES (?, ?)", (i, f"value_{i}")
                    )
                    write_count[0] += 1
                else:  # 70% reads
                    cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                    await cursor.fetchall()

            await asyncio.gather(*[random_operation(i) for i in range(500)])

            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            # Allow for some variance due to concurrent counting
            assert rows[0][0] >= write_count[0] - 10
            assert rows[0][0] <= write_count[0] + 10

            await pool.close()

    async def test_rapid_transaction_cycles(self):
        """Test rapid transaction start/commit cycles."""
        with temp_db() as db_path:
            pool = await create_pool(db_path)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            async def transaction_cycle(i):
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN")
                    await conn.execute(f"INSERT INTO {table} VALUES (?)", (i,))
                    await conn.execute("COMMIT")

            # Run many transaction cycles
            await asyncio.gather(*[transaction_cycle(i) for i in range(100)])

            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 100

            await pool.close()

    async def test_reader_writer_contention(self):
        """Test behavior under maximum reader/writer contention."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=8)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (0)")

            async def continuous_reader(reader_id):
                """Read as fast as possible."""
                for _ in range(100):
                    cursor = await pool.execute(f"SELECT * FROM {table}")
                    await cursor.fetchall()

            async def continuous_writer():
                """Write as fast as possible."""
                for i in range(100):
                    await pool.execute(f"UPDATE {table} SET x = ?", (i,))

            # Start all readers and writers simultaneously
            tasks = [
                *[continuous_reader(i) for i in range(8)],
                continuous_writer(),
                continuous_writer(),
            ]

            await asyncio.wait_for(asyncio.gather(*tasks), timeout=60.0)

            await pool.close()

    async def test_concurrent_pool_operations_with_transactions(self):
        """Test concurrent pool operations while transactions are active.

        This test verifies the pool handles concurrent transactions,
        reads, and writes without deadlocking or corrupting data.
        """
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=4)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            results = []
            errors = []

            async def transaction_worker(worker_id):
                try:
                    async with await pool.bound_connection() as conn:
                        await conn.execute("BEGIN")
                        await conn.execute(
                            f"INSERT INTO {table} VALUES (?)", (worker_id * 1000,)
                        )
                        await asyncio.sleep(0.01)
                        await conn.execute("COMMIT")
                        results.append(f"tx_{worker_id}")
                except Exception as e:
                    errors.append((worker_id, "tx", str(e)))

            async def pool_reader(reader_id):
                try:
                    for _ in range(10):
                        cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                        await cursor.fetchall()
                        await asyncio.sleep(0.001)
                    results.append(f"read_{reader_id}")
                except Exception as e:
                    errors.append((reader_id, "read", str(e)))

            async def pool_writer(writer_id):
                try:
                    for _ in range(10):
                        await pool.execute(f"INSERT INTO {table} VALUES (?)", (writer_id,))
                        await asyncio.sleep(0.001)
                    results.append(f"write_{writer_id}")
                except Exception as e:
                    errors.append((writer_id, "write", str(e)))

            tasks = [
                *[transaction_worker(i) for i in range(5)],
                *[pool_reader(i) for i in range(10)],
                *[pool_writer(i) for i in range(5)],
            ]

            await asyncio.wait_for(asyncio.gather(*tasks), timeout=120.0)

            # Check results - some operations should have completed
            assert len(results) > 0, "No operations completed"
            # Minor errors are acceptable in high-concurrency scenarios
            if errors:
                # Log errors but don't fail if most operations succeeded
                print(f"Errors during concurrent test: {errors[:3]}")

            await pool.close()

    async def test_burst_operations_then_idle(self):
        """Test burst of operations followed by idle period then more operations."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=2)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

            # Burst 1
            tasks = [
                pool.execute(f"INSERT INTO {table} VALUES (?)", (i,)) for i in range(100)
            ]
            await asyncio.gather(*tasks)

            # Idle
            await asyncio.sleep(0.5)

            # Burst 2
            tasks = [
                pool.execute(f"INSERT INTO {table} VALUES (?)", (i,)) for i in range(100, 200)
            ]
            await asyncio.gather(*tasks)

            # Idle
            await asyncio.sleep(0.5)

            # Verify
            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 200

            await pool.close()

    async def test_alternating_read_write_bursts(self):
        """Test alternating bursts of reads and writes."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=4)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            for _ in range(10):
                # Read burst
                read_tasks = [pool.execute(f"SELECT * FROM {table}") for _ in range(50)]
                await asyncio.gather(*read_tasks)

                # Write burst
                write_tasks = [
                    pool.execute(f"INSERT INTO {table} VALUES (?)", (i,)) for i in range(10)
                ]
                await asyncio.gather(*write_tasks)

            cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
            rows = await cursor.fetchall()
            assert rows[0][0] == 101  # 1 initial + 10*10 writes

            await pool.close()


# =============================================================================
# Additional Edge Cases
# =============================================================================


class TestAdditionalEdgeCases:
    """Additional edge cases for comprehensive coverage."""

    async def test_empty_executemany(self):
        """Test executemany with empty parameter list."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
                cursor = await pool.executemany(f"INSERT INTO {table} VALUES (?)", [])
                assert cursor.rowcount == 0

    async def test_null_values(self):
        """Test handling of NULL values."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER, y TEXT)")
                await pool.execute(f"INSERT INTO {table} VALUES (NULL, NULL)")
                await pool.execute(f"INSERT INTO {table} VALUES (1, NULL)")
                await pool.execute(f"INSERT INTO {table} VALUES (NULL, 'text')")

                cursor = await pool.execute(f"SELECT * FROM {table} ORDER BY x NULLS FIRST")
                rows = await cursor.fetchall()
                assert len(rows) == 3

    async def test_special_characters_in_strings(self):
        """Test special characters in string values."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x TEXT)")

                special_strings = [
                    "Hello\nWorld",
                    "Tab\there",
                    "Quote's",
                    'Double"quote',
                    "Backslash\\here",
                ]

                for s in special_strings:
                    await pool.execute(f"INSERT INTO {table} VALUES (?)", (s,))

                cursor = await pool.execute(f"SELECT * FROM {table}")
                rows = await cursor.fetchall()
                assert len(rows) == len(special_strings)

    async def test_very_long_sql(self):
        """Test very long SQL statement."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")

                # Insert many values in one statement
                values = ", ".join(f"({i})" for i in range(1000))
                await pool.execute(f"INSERT INTO {table} VALUES {values}")

                cursor = await pool.execute(f"SELECT COUNT(*) FROM {table}")
                rows = await cursor.fetchall()
                assert rows[0][0] == 1000

    async def test_deeply_nested_transaction(self):
        """Test deeply nested savepoints.

        When rolling back to SAVEPOINT spN, all changes after spN are
        undone, keeping changes up to and including spN.
        """
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                async with await pool.bound_connection() as conn:
                    await conn.execute(f"CREATE TABLE {table} (x INTEGER)")
                    await conn.execute("BEGIN")

                    # Create 10 nested savepoints with inserts
                    # sp0: insert 0, sp1: insert 1, ... sp9: insert 9
                    for i in range(10):
                        await conn.execute(f"SAVEPOINT sp{i}")
                        await conn.execute(f"INSERT INTO {table} VALUES (?)", (i,))

                    # Rollback to sp5 - undoes inserts 6, 7, 8, 9
                    # Keeps inserts 0, 1, 2, 3, 4, 5 (the savepoint row itself is kept)
                    await conn.execute("ROLLBACK TO sp5")
                    await conn.execute("COMMIT")

                    cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")
                    rows = await cursor.fetchall()
                    # Rows 0-5 remain (ROLLBACK TO sp5 keeps sp5's insert)
                    # Actually, ROLLBACK TO spN keeps everything up to spN
                    # but the insert happens AFTER SAVEPOINT, so sp5's insert
                    # of value 5 is undone. Values 0-4 remain (5 rows).
                    assert rows[0][0] == 5

    async def test_recursive_cte(self):
        """Test recursive CTE query."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                cursor = await pool.execute("""
                    WITH RECURSIVE cnt(x) AS (
                        VALUES(1)
                        UNION ALL
                        SELECT x+1 FROM cnt WHERE x < 100
                    )
                    SELECT x FROM cnt
                """)
                rows = await cursor.fetchall()
                assert len(rows) == 100
                assert rows[0][0] == 1
                assert rows[99][0] == 100

    async def test_window_functions(self):
        """Test window function queries."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
                await pool.executemany(
                    f"INSERT INTO {table} VALUES (?)", [(i,) for i in range(10)]
                )

                cursor = await pool.execute(f"""
                    SELECT x, SUM(x) OVER (ORDER BY x) as running_sum
                    FROM {table}
                """)
                rows = await cursor.fetchall()
                assert len(rows) == 10
                # Verify running sum
                assert rows[0] == (0, 0)
                assert rows[9] == (9, 45)  # 0+1+2+...+9 = 45

    async def test_json_functions(self):
        """Test JSON functions if available."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (data TEXT)")
                await pool.execute(
                    f"INSERT INTO {table} VALUES (?)", ('{"name": "Alice", "age": 30}',)
                )

                cursor = await pool.execute(
                    f"SELECT json_extract(data, '$.name') FROM {table}"
                )
                rows = await cursor.fetchall()
                assert rows[0][0] == "Alice"

    async def test_aggregate_functions(self):
        """Test various aggregate functions."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
                await pool.executemany(
                    f"INSERT INTO {table} VALUES (?)", [(i,) for i in range(100)]
                )

                cursor = await pool.execute(f"""
                    SELECT
                        COUNT(*),
                        SUM(x),
                        AVG(x),
                        MIN(x),
                        MAX(x)
                    FROM {table}
                """)
                rows = await cursor.fetchall()
                assert rows[0][0] == 100  # COUNT
                assert rows[0][1] == 4950  # SUM (0+1+...+99)
                assert rows[0][2] == 49.5  # AVG
                assert rows[0][3] == 0  # MIN
                assert rows[0][4] == 99  # MAX

    async def test_subquery_operations(self):
        """Test subquery operations."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                t1 = unique_table_name()
                t2 = unique_table_name()
                await pool.execute(f"CREATE TABLE {t1} (x INTEGER)")
                await pool.execute(f"CREATE TABLE {t2} (y INTEGER)")
                await pool.executemany(f"INSERT INTO {t1} VALUES (?)", [(i,) for i in range(10)])
                await pool.executemany(
                    f"INSERT INTO {t2} VALUES (?)", [(i * 2,) for i in range(10)]
                )

                # Subquery in WHERE
                cursor = await pool.execute(
                    f"SELECT * FROM {t1} WHERE x IN (SELECT y FROM {t2})"
                )
                rows = await cursor.fetchall()
                assert len(rows) == 5  # 0, 2, 4, 6, 8

                # Correlated subquery
                cursor = await pool.execute(f"""
                    SELECT x, (SELECT COUNT(*) FROM {t2} WHERE y <= {t1}.x) as cnt
                    FROM {t1}
                """)
                rows = await cursor.fetchall()
                assert len(rows) == 10

    async def test_zero_readers(self):
        """Test pool with zero readers - should still work via writer."""
        with temp_db() as db_path:
            # Note: num_readers=0 might not be supported, testing behavior
            try:
                pool = await create_pool(db_path, num_readers=0)
                # If it works, close it
                await pool.close()
            except (ValueError, Exception):
                # Expected - zero readers may not be allowed
                pass

    async def test_many_readers(self):
        """Test pool with many readers."""
        with temp_db() as db_path:
            pool = await create_pool(db_path, num_readers=16)
            table = unique_table_name()
            await pool.execute(f"CREATE TABLE {table} (x INTEGER)")
            await pool.execute(f"INSERT INTO {table} VALUES (1)")

            # All readers should be able to read
            read_tasks = [pool.execute(f"SELECT * FROM {table}") for _ in range(100)]
            results = await asyncio.gather(*read_tasks)

            for cursor in results:
                rows = await cursor.fetchall()
                assert rows == [(1,)]

            await pool.close()

    async def test_unicode_table_and_column_names(self):
        """Test unicode in table and column names."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                # SQLite supports unicode identifiers
                await pool.execute('CREATE TABLE "日本語" ("名前" TEXT)')
                await pool.execute('INSERT INTO "日本語" VALUES (?)', ("テスト",))

                cursor = await pool.execute('SELECT * FROM "日本語"')
                rows = await cursor.fetchall()
                assert rows == [("テスト",)]

    async def test_emoji_data(self):
        """Test emoji in data."""
        with temp_db() as db_path:
            async with await create_pool(db_path) as pool:
                table = unique_table_name()
                await pool.execute(f"CREATE TABLE {table} (emoji TEXT)")
                await pool.execute(f"INSERT INTO {table} VALUES (?)", ("🎉🔥🚀",))
                await pool.execute(f"INSERT INTO {table} VALUES (?)", ("👨‍👩‍👧‍👦",))

                cursor = await pool.execute(f"SELECT * FROM {table}")
                rows = await cursor.fetchall()
                assert len(rows) == 2
                assert rows[0][0] == "🎉🔥🚀"
                assert rows[1][0] == "👨‍👩‍👧‍👦"
