"""Tests for the new PasynPool API."""

import asyncio
import tempfile
import os

import pytest

import pasyn_sqlite
from pasyn_sqlite import (
    PasynPool,
    BoundConnection,
    Cursor,
    create_pool,
    TransactionCommandError,
    PoolClosedError,
    ConnectionClosedError,
)


class TestPasynPool:
    """Tests for PasynPool basic functionality."""

    async def test_create_pool(self):
        """Test pool creation."""
        pool = await create_pool(":memory:")
        assert not pool.closed
        assert pool.num_readers == 1
        await pool.close()
        assert pool.closed

    async def test_create_pool_custom_readers(self):
        """Test pool with custom number of readers."""
        pool = await create_pool(":memory:", num_readers=5)
        assert pool.num_readers == 5
        await pool.close()

    async def test_pool_context_manager(self):
        """Test pool as async context manager."""
        async with await create_pool(":memory:") as pool:
            assert not pool.closed
        assert pool.closed

    async def test_execute_create_table(self):
        """Test executing CREATE TABLE."""
        async with await create_pool(":memory:") as pool:
            cursor = await pool.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
            )
            assert cursor is not None
            assert isinstance(cursor, Cursor)

    async def test_execute_insert_and_select(self):
        """Test INSERT and SELECT operations."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            await pool.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            await pool.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))

            cursor = await pool.execute("SELECT * FROM users ORDER BY id")
            rows = await cursor.fetchall()

            assert len(rows) == 2
            assert rows[0][1] == "Alice"
            assert rows[1][1] == "Bob"

    async def test_execute_returns_cursor_with_metadata(self):
        """Test that execute returns cursor with proper metadata."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

            cursor = await pool.execute("INSERT INTO test (name) VALUES (?)", ("Test",))
            assert cursor.lastrowid == 1
            assert cursor.rowcount == 1

            cursor = await pool.execute("SELECT id, name FROM test")
            assert cursor.description is not None
            assert len(cursor.description) == 2

    async def test_executemany(self):
        """Test executemany for bulk inserts."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

            data = [("Alice",), ("Bob",), ("Charlie",)]
            cursor = await pool.executemany(
                "INSERT INTO users (name) VALUES (?)", data
            )
            assert cursor.rowcount == 3

            cursor = await pool.execute("SELECT COUNT(*) FROM users")
            rows = await cursor.fetchall()
            assert rows[0][0] == 3

    async def test_pool_closed_error(self):
        """Test that operations on closed pool raise error."""
        pool = await create_pool(":memory:")
        await pool.close()

        with pytest.raises(PoolClosedError):
            await pool.execute("SELECT 1")

    async def test_read_routes_to_reader(self):
        """Test that SELECT queries work (routed to reader)."""
        async with await create_pool(":memory:", num_readers=2) as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            await pool.execute("INSERT INTO test VALUES (1)")

            # Multiple concurrent reads should work
            results = await asyncio.gather(
                pool.execute("SELECT * FROM test"),
                pool.execute("SELECT * FROM test"),
                pool.execute("SELECT * FROM test"),
            )
            assert len(results) == 3


class TestTransactionCommandBlocking:
    """Tests for blocking transaction commands on Pool."""

    async def test_begin_blocked(self):
        """Test that BEGIN is blocked on pool."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError) as exc_info:
                await pool.execute("BEGIN")
            assert "BEGIN" in str(exc_info.value)
            assert "bound_connection" in str(exc_info.value)

    async def test_commit_blocked(self):
        """Test that COMMIT is blocked on pool."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError) as exc_info:
                await pool.execute("COMMIT")
            assert "COMMIT" in str(exc_info.value)

    async def test_rollback_blocked(self):
        """Test that ROLLBACK is blocked on pool."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError) as exc_info:
                await pool.execute("ROLLBACK")
            assert "ROLLBACK" in str(exc_info.value)

    async def test_savepoint_blocked(self):
        """Test that SAVEPOINT is blocked on pool."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError) as exc_info:
                await pool.execute("SAVEPOINT test_sp")
            assert "SAVEPOINT" in str(exc_info.value)

    async def test_release_blocked(self):
        """Test that RELEASE is blocked on pool."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError) as exc_info:
                await pool.execute("RELEASE test_sp")
            assert "RELEASE" in str(exc_info.value)

    async def test_begin_immediate_blocked(self):
        """Test that BEGIN IMMEDIATE is blocked."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError):
                await pool.execute("BEGIN IMMEDIATE")

    async def test_begin_exclusive_blocked(self):
        """Test that BEGIN EXCLUSIVE is blocked."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError):
                await pool.execute("BEGIN EXCLUSIVE")

    async def test_executemany_begin_blocked(self):
        """Test that transaction commands are blocked in executemany."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(TransactionCommandError):
                await pool.executemany("BEGIN", [])


class TestBoundConnection:
    """Tests for BoundConnection."""

    async def test_bound_connection_context_manager(self):
        """Test bound_connection as context manager."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                assert not conn.closed
                assert isinstance(conn, BoundConnection)
            assert conn.closed

    async def test_bound_connection_execute(self):
        """Test execute on bound connection."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                await conn.execute("CREATE TABLE test (x INTEGER)")
                await conn.execute("INSERT INTO test VALUES (1)")
                cursor = await conn.execute("SELECT * FROM test")
                rows = await cursor.fetchall()
                assert rows == [(1,)]

    async def test_bound_connection_transaction(self):
        """Test transaction with BEGIN/COMMIT on bound connection."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            async with await create_pool(db_path) as pool:
                # Create table first
                await pool.execute("CREATE TABLE users (id INTEGER, name TEXT)")

                # Transaction with bound connection
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN")
                    await conn.execute("INSERT INTO users VALUES (1, 'Alice')")
                    await conn.execute("INSERT INTO users VALUES (2, 'Bob')")
                    await conn.execute("COMMIT")

                # Verify data persisted
                cursor = await pool.execute("SELECT COUNT(*) FROM users")
                rows = await cursor.fetchall()
                assert rows[0][0] == 2
        finally:
            os.unlink(db_path)

    async def test_bound_connection_rollback(self):
        """Test ROLLBACK on bound connection."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            async with await create_pool(db_path) as pool:
                await pool.execute("CREATE TABLE users (id INTEGER, name TEXT)")

                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN")
                    await conn.execute("INSERT INTO users VALUES (1, 'Alice')")
                    await conn.execute("ROLLBACK")

                # Verify data was rolled back
                cursor = await pool.execute("SELECT COUNT(*) FROM users")
                rows = await cursor.fetchall()
                assert rows[0][0] == 0
        finally:
            os.unlink(db_path)

    async def test_bound_connection_savepoint(self):
        """Test SAVEPOINT on bound connection."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                await conn.execute("CREATE TABLE test (x INTEGER)")
                await conn.execute("BEGIN")
                await conn.execute("INSERT INTO test VALUES (1)")
                await conn.execute("SAVEPOINT sp1")
                await conn.execute("INSERT INTO test VALUES (2)")
                await conn.execute("ROLLBACK TO sp1")
                await conn.execute("COMMIT")

                cursor = await conn.execute("SELECT * FROM test")
                rows = await cursor.fetchall()
                # Only first insert should remain
                assert rows == [(1,)]

    async def test_bound_connection_nested_savepoints(self):
        """Test nested savepoints."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                await conn.execute("CREATE TABLE test (x INTEGER)")
                await conn.execute("BEGIN")
                await conn.execute("INSERT INTO test VALUES (1)")
                await conn.execute("SAVEPOINT sp1")
                await conn.execute("INSERT INTO test VALUES (2)")
                await conn.execute("SAVEPOINT sp2")
                await conn.execute("INSERT INTO test VALUES (3)")
                await conn.execute("ROLLBACK TO sp2")
                await conn.execute("RELEASE sp1")
                await conn.execute("COMMIT")

                cursor = await conn.execute("SELECT * FROM test ORDER BY x")
                rows = await cursor.fetchall()
                assert rows == [(1,), (2,)]

    async def test_bound_connection_commit_method(self):
        """Test commit() method on bound connection."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            async with await create_pool(db_path) as pool:
                await pool.execute("CREATE TABLE test (x INTEGER)")

                async with await pool.bound_connection() as conn:
                    await conn.execute("INSERT INTO test VALUES (1)")
                    await conn.commit()

                cursor = await pool.execute("SELECT * FROM test")
                rows = await cursor.fetchall()
                assert rows == [(1,)]
        finally:
            os.unlink(db_path)

    async def test_bound_connection_rollback_method(self):
        """Test rollback() method on bound connection."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            async with await create_pool(db_path) as pool:
                await pool.execute("CREATE TABLE test (x INTEGER)")

                async with await pool.bound_connection() as conn:
                    await conn.execute("INSERT INTO test VALUES (1)")
                    await conn.rollback()

                cursor = await pool.execute("SELECT * FROM test")
                rows = await cursor.fetchall()
                assert rows == []
        finally:
            os.unlink(db_path)

    async def test_bound_connection_executemany(self):
        """Test executemany on bound connection."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                await conn.execute("CREATE TABLE test (x INTEGER)")
                await conn.executemany("INSERT INTO test VALUES (?)", [(1,), (2,), (3,)])
                cursor = await conn.execute("SELECT COUNT(*) FROM test")
                rows = await cursor.fetchall()
                assert rows[0][0] == 3

    async def test_bound_connection_executescript(self):
        """Test executescript on bound connection."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                await conn.executescript("""
                    CREATE TABLE t1 (x INTEGER);
                    CREATE TABLE t2 (y TEXT);
                    INSERT INTO t1 VALUES (1);
                    INSERT INTO t2 VALUES ('hello');
                """)

                cursor = await conn.execute("SELECT * FROM t1")
                assert await cursor.fetchall() == [(1,)]

                cursor = await conn.execute("SELECT * FROM t2")
                assert await cursor.fetchall() == [("hello",)]

    async def test_bound_connection_in_transaction_property(self):
        """Test in_transaction property."""
        async with await create_pool(":memory:") as pool:
            async with await pool.bound_connection() as conn:
                await conn.execute("CREATE TABLE test (x INTEGER)")

                assert not conn.in_transaction
                await conn.execute("BEGIN")
                assert conn.in_transaction
                await conn.execute("COMMIT")
                assert not conn.in_transaction

    async def test_bound_connection_auto_rollback_on_exception(self):
        """Test that uncommitted transaction is rolled back on exception."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            async with await create_pool(db_path) as pool:
                await pool.execute("CREATE TABLE test (x INTEGER)")

                with pytest.raises(ValueError):
                    async with await pool.bound_connection() as conn:
                        await conn.execute("BEGIN")
                        await conn.execute("INSERT INTO test VALUES (1)")
                        raise ValueError("Test error")

                # Data should be rolled back
                cursor = await pool.execute("SELECT COUNT(*) FROM test")
                rows = await cursor.fetchall()
                assert rows[0][0] == 0
        finally:
            os.unlink(db_path)

    async def test_bound_connection_serialization(self):
        """Test that bound connections serialize access to writer."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE counter (value INTEGER)")
            await pool.execute("INSERT INTO counter VALUES (0)")

            async def increment():
                async with await pool.bound_connection() as conn:
                    await conn.execute("BEGIN IMMEDIATE")
                    cursor = await conn.execute("SELECT value FROM counter")
                    rows = await cursor.fetchall()
                    value = rows[0][0]
                    await conn.execute("UPDATE counter SET value = ?", (value + 1,))
                    await conn.execute("COMMIT")

            # Run 10 concurrent increments
            await asyncio.gather(*[increment() for _ in range(10)])

            cursor = await pool.execute("SELECT value FROM counter")
            rows = await cursor.fetchall()
            assert rows[0][0] == 10


class TestCursor:
    """Tests for Cursor functionality."""

    async def test_cursor_fetchone(self):
        """Test fetchone."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            await pool.execute("INSERT INTO test VALUES (1)")
            await pool.execute("INSERT INTO test VALUES (2)")

            cursor = await pool.execute("SELECT * FROM test ORDER BY x")
            assert await cursor.fetchone() == (1,)
            assert await cursor.fetchone() == (2,)
            assert await cursor.fetchone() is None

    async def test_cursor_fetchmany(self):
        """Test fetchmany."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            for i in range(5):
                await pool.execute("INSERT INTO test VALUES (?)", (i,))

            cursor = await pool.execute("SELECT * FROM test ORDER BY x")
            rows = await cursor.fetchmany(2)
            assert rows == [(0,), (1,)]
            rows = await cursor.fetchmany(2)
            assert rows == [(2,), (3,)]
            rows = await cursor.fetchmany(2)
            assert rows == [(4,)]

    async def test_cursor_fetchall(self):
        """Test fetchall."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            for i in range(3):
                await pool.execute("INSERT INTO test VALUES (?)", (i,))

            cursor = await pool.execute("SELECT * FROM test ORDER BY x")
            rows = await cursor.fetchall()
            assert rows == [(0,), (1,), (2,)]

    async def test_cursor_iteration(self):
        """Test async iteration over cursor."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            for i in range(3):
                await pool.execute("INSERT INTO test VALUES (?)", (i,))

            cursor = await pool.execute("SELECT * FROM test ORDER BY x")
            rows = []
            async for row in cursor:
                rows.append(row)
            assert rows == [(0,), (1,), (2,)]

    async def test_cursor_context_manager(self):
        """Test cursor as context manager."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")

            async with await pool.execute("SELECT 1") as cursor:
                assert not cursor.closed
            assert cursor.closed

    async def test_cursor_arraysize(self):
        """Test cursor arraysize property."""
        async with await create_pool(":memory:") as pool:
            cursor = await pool.execute("SELECT 1")
            assert cursor.arraysize == 1
            cursor.arraysize = 10
            assert cursor.arraysize == 10


class TestConcurrency:
    """Tests for concurrent operations."""

    async def test_concurrent_reads(self):
        """Test concurrent read operations."""
        async with await create_pool(":memory:", num_readers=3) as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            for i in range(100):
                await pool.execute("INSERT INTO test VALUES (?)", (i,))

            async def read():
                cursor = await pool.execute("SELECT SUM(x) FROM test")
                return await cursor.fetchone()

            results = await asyncio.gather(*[read() for _ in range(20)])
            expected = sum(range(100))
            assert all(r[0] == expected for r in results)

    async def test_concurrent_writes(self):
        """Test concurrent write operations (serialized through writer)."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")

            async def write(value):
                await pool.execute("INSERT INTO test VALUES (?)", (value,))

            await asyncio.gather(*[write(i) for i in range(50)])

            cursor = await pool.execute("SELECT COUNT(*) FROM test")
            rows = await cursor.fetchall()
            assert rows[0][0] == 50

    async def test_mixed_operations(self):
        """Test mixed read/write operations."""
        # Use file-based database for better WAL concurrency
        # (shared cache in-memory DBs have stricter locking)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            async with await create_pool(db_path, num_readers=3) as pool:
                await pool.execute("CREATE TABLE test (x INTEGER)")
                await pool.execute("INSERT INTO test VALUES (1)")

                async def read():
                    cursor = await pool.execute("SELECT * FROM test")
                    return await cursor.fetchall()

                async def write(value):
                    await pool.execute("INSERT INTO test VALUES (?)", (value,))

                # Mix reads and writes
                tasks = []
                for i in range(10):
                    tasks.append(read())
                    tasks.append(write(i + 2))

                await asyncio.gather(*tasks)

                cursor = await pool.execute("SELECT COUNT(*) FROM test")
                rows = await cursor.fetchall()
                assert rows[0][0] == 11  # 1 initial + 10 inserts
        finally:
            os.unlink(db_path)


class TestEdgeCases:
    """Tests for edge cases."""

    async def test_empty_result(self):
        """Test handling of empty result sets."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (x INTEGER)")
            cursor = await pool.execute("SELECT * FROM test")
            rows = await cursor.fetchall()
            assert rows == []

    async def test_unicode(self):
        """Test unicode handling."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (name TEXT)")
            await pool.execute("INSERT INTO test VALUES (?)", ("日本語",))
            await pool.execute("INSERT INTO test VALUES (?)", ("🎉",))

            cursor = await pool.execute("SELECT * FROM test ORDER BY name")
            rows = await cursor.fetchall()
            assert ("日本語",) in rows
            assert ("🎉",) in rows

    async def test_blob(self):
        """Test blob handling."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (data BLOB)")
            data = b"\x00\x01\x02\xff"
            await pool.execute("INSERT INTO test VALUES (?)", (data,))

            cursor = await pool.execute("SELECT * FROM test")
            rows = await cursor.fetchall()
            assert rows[0][0] == data

    async def test_sql_error(self):
        """Test SQL error handling."""
        async with await create_pool(":memory:") as pool:
            with pytest.raises(Exception):  # sqlite3.OperationalError
                await pool.execute("SELECT * FROM nonexistent")

    async def test_parameter_binding(self):
        """Test various parameter binding styles."""
        async with await create_pool(":memory:") as pool:
            await pool.execute("CREATE TABLE test (a INTEGER, b TEXT)")

            # Positional
            await pool.execute("INSERT INTO test VALUES (?, ?)", (1, "one"))

            # Named
            await pool.execute(
                "INSERT INTO test VALUES (:a, :b)", {"a": 2, "b": "two"}
            )

            cursor = await pool.execute("SELECT * FROM test ORDER BY a")
            rows = await cursor.fetchall()
            assert rows == [(1, "one"), (2, "two")]

    async def test_case_insensitive_transaction_detection(self):
        """Test that transaction commands are detected case-insensitively."""
        async with await create_pool(":memory:") as pool:
            for cmd in ["begin", "BEGIN", "Begin", "  BEGIN  "]:
                with pytest.raises(TransactionCommandError):
                    await pool.execute(cmd)

    async def test_file_database(self):
        """Test with file-based database."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as f:
            db_path = f.name

        try:
            # Create and populate
            async with await create_pool(db_path) as pool:
                await pool.execute("CREATE TABLE test (x INTEGER)")
                await pool.execute("INSERT INTO test VALUES (42)")

            # Reopen and verify
            async with await create_pool(db_path) as pool:
                cursor = await pool.execute("SELECT * FROM test")
                rows = await cursor.fetchall()
                assert rows == [(42,)]
        finally:
            os.unlink(db_path)
