"""Tests for pasyn-sqlite."""

import asyncio
import tempfile
from pathlib import Path

import pytest

import pasyn_sqlite
from pasyn_sqlite import Connection, Cursor, ThreadPool
from pasyn_sqlite.exceptions import ConnectionClosedError, PoolClosedError


class TestThreadPool:
    """Tests for the ThreadPool class."""

    def test_pool_creation(self) -> None:
        """Test that pool creates correctly with default settings."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            with ThreadPool(db_path) as pool:
                assert pool.num_readers == 3
                assert not pool.closed
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_pool_custom_readers(self) -> None:
        """Test pool with custom number of readers."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            with ThreadPool(db_path, num_readers=5) as pool:
                assert pool.num_readers == 5
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_pool_close(self) -> None:
        """Test that pool closes correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            pool = ThreadPool(db_path)
            assert not pool.closed
            pool.close()
            assert pool.closed
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_pool_closed_error(self) -> None:
        """Test that operations on closed pool raise error."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            pool = ThreadPool(db_path)
            pool.close()

            with pytest.raises(PoolClosedError):
                pool.submit_write(lambda conn: None)

            with pytest.raises(PoolClosedError):
                pool.submit_read(lambda conn: None)
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_is_read_operation(self) -> None:
        """Test SQL statement classification."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            with ThreadPool(db_path) as pool:
                # Read operations
                assert pool._is_read_operation("SELECT * FROM users")
                assert pool._is_read_operation("  select id from users  ")
                assert pool._is_read_operation("WITH cte AS (SELECT 1) SELECT * FROM cte")

                # Write operations
                assert not pool._is_read_operation("INSERT INTO users VALUES (1)")
                assert not pool._is_read_operation("UPDATE users SET name = 'x'")
                assert not pool._is_read_operation("DELETE FROM users")
                assert not pool._is_read_operation("CREATE TABLE foo (id INT)")
                assert not pool._is_read_operation("DROP TABLE foo")
                assert not pool._is_read_operation("BEGIN TRANSACTION")
                assert not pool._is_read_operation("COMMIT")
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_submit_write(self) -> None:
        """Test submitting a write task."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            with ThreadPool(db_path) as pool:

                def create_table(conn):
                    conn.execute("CREATE TABLE test (id INTEGER)")
                    conn.commit()
                    return "done"

                future = pool.submit_write(create_table)
                result = future.result(timeout=5)
                assert result == "done"
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_submit_read(self) -> None:
        """Test submitting a read task."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            with ThreadPool(db_path) as pool:
                # Create table first
                def setup(conn):
                    conn.execute("CREATE TABLE test (id INTEGER)")
                    conn.execute("INSERT INTO test VALUES (1), (2), (3)")
                    conn.commit()

                pool.submit_write(setup).result(timeout=5)

                # Read data
                def read_data(conn):
                    cursor = conn.execute("SELECT * FROM test")
                    return cursor.fetchall()

                future = pool.submit_read(read_data)
                result = future.result(timeout=5)
                assert result == [(1,), (2,), (3,)]
        finally:
            Path(db_path).unlink(missing_ok=True)


class TestConnection:
    """Tests for the async Connection class."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> str:
        """Create a temporary database path."""
        return str(tmp_path / "test.db")

    @pytest.mark.asyncio
    async def test_connect(self, db_path: str) -> None:
        """Test creating a connection."""
        conn = await pasyn_sqlite.connect(db_path)
        assert not conn.closed
        await conn.close()
        assert conn.closed

    @pytest.mark.asyncio
    async def test_context_manager(self, db_path: str) -> None:
        """Test connection as context manager."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            assert not conn.closed
        assert conn.closed

    @pytest.mark.asyncio
    async def test_execute_create_table(self, db_path: str) -> None:
        """Test executing CREATE TABLE."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
            )
            await conn.commit()

            # Verify table exists
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
            )
            rows = await cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "users"

    @pytest.mark.asyncio
    async def test_execute_insert_and_select(self, db_path: str) -> None:
        """Test INSERT and SELECT operations."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM users ORDER BY id")
            rows = await cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][1] == "Alice"
            assert rows[1][1] == "Bob"

    @pytest.mark.asyncio
    async def test_executemany(self, db_path: str) -> None:
        """Test executemany operation."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

            users = [("Alice",), ("Bob",), ("Charlie",)]
            await conn.executemany("INSERT INTO users (name) VALUES (?)", users)
            await conn.commit()

            cursor = await conn.execute("SELECT COUNT(*) FROM users")
            row = await cursor.fetchone()
            assert row[0] == 3

    @pytest.mark.asyncio
    async def test_executescript(self, db_path: str) -> None:
        """Test executescript operation."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            script = """
                CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO users (name) VALUES ('Alice');
                INSERT INTO users (name) VALUES ('Bob');
            """
            await conn.executescript(script)

            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
            assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_rollback(self, db_path: str) -> None:
        """Test rollback operation."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            await conn.commit()

            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            await conn.rollback()

            cursor = await conn.execute("SELECT COUNT(*) FROM users")
            row = await cursor.fetchone()
            assert row[0] == 0

    @pytest.mark.asyncio
    async def test_closed_connection_error(self, db_path: str) -> None:
        """Test that operations on closed connection raise error."""
        conn = await pasyn_sqlite.connect(db_path)
        await conn.close()

        with pytest.raises(ConnectionClosedError):
            await conn.execute("SELECT 1")

    @pytest.mark.asyncio
    async def test_custom_readers(self, db_path: str) -> None:
        """Test connection with custom number of readers."""
        async with await pasyn_sqlite.connect(db_path, num_readers=5) as conn:
            assert conn._pool.num_readers == 5


class TestCursor:
    """Tests for the async Cursor class."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> str:
        """Create a temporary database path."""
        return str(tmp_path / "test.db")

    @pytest.mark.asyncio
    async def test_fetchone(self, db_path: str) -> None:
        """Test fetchone operation."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.executemany("INSERT INTO nums VALUES (?)", [(1,), (2,), (3,)])
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM nums ORDER BY n")
            row1 = await cursor.fetchone()
            row2 = await cursor.fetchone()
            row3 = await cursor.fetchone()
            row4 = await cursor.fetchone()

            assert row1 == (1,)
            assert row2 == (2,)
            assert row3 == (3,)
            assert row4 is None

    @pytest.mark.asyncio
    async def test_fetchmany(self, db_path: str) -> None:
        """Test fetchmany operation."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.executemany(
                "INSERT INTO nums VALUES (?)",
                [(i,) for i in range(10)],
            )
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM nums ORDER BY n")
            rows1 = await cursor.fetchmany(3)
            rows2 = await cursor.fetchmany(3)
            rows3 = await cursor.fetchmany(5)

            assert len(rows1) == 3
            assert len(rows2) == 3
            assert len(rows3) == 4  # Only 4 remaining

    @pytest.mark.asyncio
    async def test_fetchall(self, db_path: str) -> None:
        """Test fetchall operation."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.executemany(
                "INSERT INTO nums VALUES (?)",
                [(i,) for i in range(5)],
            )
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM nums ORDER BY n")
            rows = await cursor.fetchall()

            assert len(rows) == 5
            assert rows == [(0,), (1,), (2,), (3,), (4,)]

    @pytest.mark.asyncio
    async def test_cursor_iteration(self, db_path: str) -> None:
        """Test async iteration over cursor."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.executemany(
                "INSERT INTO nums VALUES (?)",
                [(i,) for i in range(5)],
            )
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM nums ORDER BY n")
            results = []
            async for row in cursor:
                results.append(row)

            assert len(results) == 5
            assert results == [(0,), (1,), (2,), (3,), (4,)]

    @pytest.mark.asyncio
    async def test_cursor_description(self, db_path: str) -> None:
        """Test cursor description property."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
            await conn.commit()

            cursor = await conn.execute("SELECT id, name FROM users")
            assert cursor.description is not None
            assert len(cursor.description) == 2
            assert cursor.description[0][0] == "id"
            assert cursor.description[1][0] == "name"

    @pytest.mark.asyncio
    async def test_cursor_rowcount(self, db_path: str) -> None:
        """Test cursor rowcount property."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.commit()

            cursor = await conn.executemany(
                "INSERT INTO nums VALUES (?)",
                [(i,) for i in range(5)],
            )
            # Note: rowcount for INSERT is implementation-dependent
            assert cursor.rowcount >= 0

    @pytest.mark.asyncio
    async def test_cursor_lastrowid(self, db_path: str) -> None:
        """Test cursor lastrowid property."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            cursor = await conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))

            assert cursor.lastrowid is not None
            assert cursor.lastrowid == 1

    @pytest.mark.asyncio
    async def test_cursor_context_manager(self, db_path: str) -> None:
        """Test cursor as context manager."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.commit()

            async with await conn.execute("SELECT * FROM nums") as cursor:
                assert not cursor.closed
            assert cursor.closed

    @pytest.mark.asyncio
    async def test_cursor_arraysize(self, db_path: str) -> None:
        """Test cursor arraysize property."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            cursor = await conn.cursor()
            assert cursor.arraysize == 1

            cursor.arraysize = 10
            assert cursor.arraysize == 10


class TestConcurrency:
    """Tests for concurrent operations."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> str:
        """Create a temporary database path."""
        return str(tmp_path / "test.db")

    @pytest.mark.asyncio
    async def test_concurrent_reads(self, db_path: str) -> None:
        """Test multiple concurrent read operations."""
        async with await pasyn_sqlite.connect(db_path, num_readers=3) as conn:
            await conn.execute("CREATE TABLE nums (n INTEGER)")
            await conn.executemany(
                "INSERT INTO nums VALUES (?)",
                [(i,) for i in range(100)],
            )
            await conn.commit()

            async def read_all():
                cursor = await conn.execute("SELECT * FROM nums")
                return await cursor.fetchall()

            # Run 10 concurrent reads
            results = await asyncio.gather(*[read_all() for _ in range(10)])

            for result in results:
                assert len(result) == 100

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, db_path: str) -> None:
        """Test multiple concurrent write operations (serialized by writer thread)."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)")
            await conn.execute("INSERT INTO counter VALUES (1, 0)")
            await conn.commit()

            async def increment():
                await conn.execute("UPDATE counter SET value = value + 1 WHERE id = 1")
                await conn.commit()

            # Run 50 concurrent increments
            await asyncio.gather(*[increment() for _ in range(50)])

            cursor = await conn.execute("SELECT value FROM counter WHERE id = 1")
            row = await cursor.fetchone()
            assert row[0] == 50

    @pytest.mark.asyncio
    async def test_mixed_read_write(self, db_path: str) -> None:
        """Test mixed read and write operations."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
            await conn.commit()

            async def write_item(i: int):
                await conn.execute("INSERT INTO items (data) VALUES (?)", (f"item-{i}",))
                await conn.commit()

            async def read_items():
                cursor = await conn.execute("SELECT * FROM items")
                return await cursor.fetchall()

            # Interleave writes and reads
            tasks = []
            for i in range(10):
                tasks.append(write_item(i))
                tasks.append(read_items())

            await asyncio.gather(*tasks)

            # Final count should be 10
            cursor = await conn.execute("SELECT COUNT(*) FROM items")
            row = await cursor.fetchone()
            assert row[0] == 10


class TestWorkStealing:
    """Tests for work-stealing behavior."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> str:
        """Create a temporary database path."""
        return str(tmp_path / "test.db")

    @pytest.mark.asyncio
    async def test_work_stealing_under_load(self, db_path: str) -> None:
        """Test that work stealing distributes load across readers."""
        async with await pasyn_sqlite.connect(db_path, num_readers=3) as conn:
            await conn.execute("CREATE TABLE data (id INTEGER, value TEXT)")
            await conn.executemany(
                "INSERT INTO data VALUES (?, ?)",
                [(i, f"value-{i}") for i in range(1000)],
            )
            await conn.commit()

            async def heavy_read():
                # Simulate a heavier read
                cursor = await conn.execute(
                    "SELECT * FROM data WHERE id >= ? ORDER BY value",
                    (0,),
                )
                return await cursor.fetchall()

            # Submit many reads - should be distributed via work stealing
            results = await asyncio.gather(*[heavy_read() for _ in range(20)])

            for result in results:
                assert len(result) == 1000


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> str:
        """Create a temporary database path."""
        return str(tmp_path / "test.db")

    @pytest.mark.asyncio
    async def test_memory_database(self) -> None:
        """Test in-memory database."""
        # Note: Each thread has its own :memory: database
        # This is a limitation of the current design
        async with await pasyn_sqlite.connect(":memory:") as conn:
            await conn.execute("CREATE TABLE test (id INTEGER)")
            await conn.commit()

    @pytest.mark.asyncio
    async def test_empty_result(self, db_path: str) -> None:
        """Test handling of empty result sets."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE users (id INTEGER)")
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
            assert rows == []

            row = await cursor.fetchone()
            assert row is None

    @pytest.mark.asyncio
    async def test_sql_error(self, db_path: str) -> None:
        """Test handling of SQL errors."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            with pytest.raises(pasyn_sqlite.OperationalError):
                await conn.execute("SELECT * FROM nonexistent_table")

    @pytest.mark.asyncio
    async def test_parameter_binding(self, db_path: str) -> None:
        """Test various parameter binding styles."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (a TEXT, b TEXT, c TEXT)")
            await conn.commit()

            # Positional parameters
            await conn.execute(
                "INSERT INTO test VALUES (?, ?, ?)",
                ("x", "y", "z"),
            )

            # Named parameters
            await conn.execute(
                "INSERT INTO test VALUES (:a, :b, :c)",
                {"a": "1", "b": "2", "c": "3"},
            )

            await conn.commit()

            cursor = await conn.execute("SELECT * FROM test")
            rows = await cursor.fetchall()
            assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_unicode(self, db_path: str) -> None:
        """Test Unicode handling."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (text TEXT)")
            await conn.commit()

            unicode_text = "Hello, 世界! 🌍 Привет мир"
            await conn.execute("INSERT INTO test VALUES (?)", (unicode_text,))
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM test")
            row = await cursor.fetchone()
            assert row[0] == unicode_text

    @pytest.mark.asyncio
    async def test_blob(self, db_path: str) -> None:
        """Test BLOB handling."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (data BLOB)")
            await conn.commit()

            binary_data = bytes(range(256))
            await conn.execute("INSERT INTO test VALUES (?)", (binary_data,))
            await conn.commit()

            cursor = await conn.execute("SELECT * FROM test")
            row = await cursor.fetchone()
            assert row[0] == binary_data


class TestTransactions:
    """Test transaction support."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> str:
        """Create a temporary database path."""
        return str(tmp_path / "test.db")

    @pytest.mark.asyncio
    async def test_basic_transaction_commit(self, db_path: str) -> None:
        """Test basic transaction with automatic commit."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            await conn.commit()

            async with conn.transaction():
                await conn.execute("INSERT INTO test (value) VALUES (?)", ("one",))
                await conn.execute("INSERT INTO test (value) VALUES (?)", ("two",))
            # Auto-committed

            cursor = await conn.execute("SELECT COUNT(*) FROM test")
            row = await cursor.fetchone()
            assert row[0] == 2

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_exception(self, db_path: str) -> None:
        """Test transaction rollback on exception."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            await conn.commit()

            # Insert initial value
            await conn.execute("INSERT INTO test (value) VALUES (?)", ("original",))
            await conn.commit()

            # Try a transaction that fails
            try:
                async with conn.transaction():
                    await conn.execute("INSERT INTO test (value) VALUES (?)", ("new",))
                    raise ValueError("Simulated error")
            except ValueError:
                pass  # Expected

            # Should only have the original value
            cursor = await conn.execute("SELECT COUNT(*) FROM test")
            row = await cursor.fetchone()
            assert row[0] == 1

            cursor = await conn.execute("SELECT value FROM test")
            row = await cursor.fetchone()
            assert row[0] == "original"

    @pytest.mark.asyncio
    async def test_nested_transaction_with_savepoint(self, db_path: str) -> None:
        """Test nested transactions use savepoints."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            await conn.commit()

            async with conn.transaction():
                await conn.execute("INSERT INTO test (value) VALUES (?)", ("outer",))

                # Nested transaction (savepoint)
                try:
                    async with conn.transaction():
                        await conn.execute("INSERT INTO test (value) VALUES (?)", ("inner",))
                        raise ValueError("Inner error")
                except ValueError:
                    pass  # Inner transaction rolled back

                # Outer transaction continues
                await conn.execute("INSERT INTO test (value) VALUES (?)", ("after",))
            # Outer commits

            cursor = await conn.execute("SELECT value FROM test ORDER BY id")
            rows = await cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "outer"
            assert rows[1][0] == "after"

    @pytest.mark.asyncio
    async def test_savepoint_explicit(self, db_path: str) -> None:
        """Test explicit savepoint context manager."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            await conn.commit()

            async with conn.transaction():
                await conn.execute("INSERT INTO test (value) VALUES (?)", ("one",))

                async with conn.savepoint("sp1"):
                    await conn.execute("INSERT INTO test (value) VALUES (?)", ("two",))
                # Savepoint released (committed)

                try:
                    async with conn.savepoint("sp2"):
                        await conn.execute("INSERT INTO test (value) VALUES (?)", ("three",))
                        raise ValueError("Rollback sp2")
                except ValueError:
                    pass  # sp2 rolled back

                await conn.execute("INSERT INTO test (value) VALUES (?)", ("four",))
            # Transaction committed

            cursor = await conn.execute("SELECT value FROM test ORDER BY id")
            rows = await cursor.fetchall()
            assert len(rows) == 3
            assert [r[0] for r in rows] == ["one", "two", "four"]

    @pytest.mark.asyncio
    async def test_transaction_isolation_levels(self, db_path: str) -> None:
        """Test different isolation levels."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            await conn.commit()

            # Test DEFERRED (default)
            async with conn.transaction(isolation=pasyn_sqlite.IsolationLevel.DEFERRED):
                await conn.execute("INSERT INTO test DEFAULT VALUES")

            # Test IMMEDIATE
            async with conn.transaction(isolation=pasyn_sqlite.IsolationLevel.IMMEDIATE):
                await conn.execute("INSERT INTO test DEFAULT VALUES")

            # Test EXCLUSIVE
            async with conn.transaction(isolation=pasyn_sqlite.IsolationLevel.EXCLUSIVE):
                await conn.execute("INSERT INTO test DEFAULT VALUES")

            cursor = await conn.execute("SELECT COUNT(*) FROM test")
            row = await cursor.fetchone()
            assert row[0] == 3

    @pytest.mark.asyncio
    async def test_reads_in_transaction_go_to_writer(self, db_path: str) -> None:
        """Test that reads within transaction use writer thread."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            await conn.commit()

            async with conn.transaction():
                await conn.execute("INSERT INTO test (value) VALUES (?)", ("one",))

                # This read should see the uncommitted data
                # because it's on the same connection (writer)
                cursor = await conn.execute("SELECT COUNT(*) FROM test")
                row = await cursor.fetchone()
                assert row[0] == 1

    @pytest.mark.asyncio
    async def test_concurrent_transactions_serialize(self, db_path: str) -> None:
        """Test that concurrent transactions serialize properly."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
            await conn.commit()

            results = []

            async def transaction_task(value: int) -> None:
                async with conn.transaction():
                    await conn.execute("INSERT INTO test (value) VALUES (?)", (value,))
                    await asyncio.sleep(0.01)  # Simulate work
                    cursor = await conn.execute("SELECT MAX(id) FROM test")
                    row = await cursor.fetchone()
                    results.append((value, row[0]))

            # Start multiple transactions concurrently
            await asyncio.gather(
                transaction_task(1),
                transaction_task(2),
                transaction_task(3),
            )

            # All should have completed
            cursor = await conn.execute("SELECT COUNT(*) FROM test")
            row = await cursor.fetchone()
            assert row[0] == 3

            # Each transaction should have seen consistent state
            assert len(results) == 3

    @pytest.mark.asyncio
    async def test_savepoint_outside_transaction_error(self, db_path: str) -> None:
        """Test that savepoint outside transaction raises error."""
        async with await pasyn_sqlite.connect(db_path) as conn:
            with pytest.raises(pasyn_sqlite.NoActiveTransactionError):
                async with conn.savepoint("sp1"):
                    pass

    @pytest.mark.asyncio
    async def test_transaction_close_connection(self, db_path: str) -> None:
        """Test that closing connection with active transaction rollbacks."""
        conn = await pasyn_sqlite.connect(db_path)
        await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        await conn.commit()

        # Start a transaction
        tx = conn.transaction()
        await tx.__aenter__()
        await conn.execute("INSERT INTO test DEFAULT VALUES")

        # Close connection without exiting transaction
        await conn.close()

        # Verify rollback happened by opening new connection
        async with await pasyn_sqlite.connect(db_path) as conn2:
            cursor = await conn2.execute("SELECT COUNT(*) FROM test")
            row = await cursor.fetchone()
            assert row[0] == 0  # Rolled back
