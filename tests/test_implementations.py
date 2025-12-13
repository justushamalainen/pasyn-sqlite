"""
Tests for all SQLite client implementations.

This module tests MainThreadSQLite, APSWMainThreadSQLite, Pysqlite3MainThreadSQLite,
and MultiplexedSQLite implementations with common test scenarios.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Add benchmarks to path for implementations
sys.path.insert(0, str(Path(__file__).parent.parent / "benchmarks"))

from implementations import (
    APSWMainThreadSQLite,
    BaseSQLiteImplementation,
    MainThreadSQLite,
    MultiplexedSQLite,
    Pysqlite3MainThreadSQLite,
)


@pytest.fixture
def db_path():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    try:
        os.unlink(path)
    except OSError:
        pass
    # Cleanup WAL files
    for suffix in ["-wal", "-shm"]:
        try:
            os.unlink(path + suffix)
        except OSError:
            pass
    # Cleanup socket file for multiplexed
    socket_path = path + ".sock"
    try:
        os.unlink(socket_path)
    except OSError:
        pass


@pytest.fixture(params=[
    MainThreadSQLite,
    APSWMainThreadSQLite,
    Pysqlite3MainThreadSQLite,
    MultiplexedSQLite,
], ids=["main_thread", "apsw_main_thread", "pysqlite3_main_thread", "multiplexed"])
async def impl(request, db_path):
    """Fixture that provides each implementation for testing."""
    impl_class = request.param
    instance = impl_class()
    await instance.setup(db_path)
    yield instance
    await instance.close()


class TestBasicOperations:
    """Tests for basic CRUD operations."""

    async def test_create_table(self, impl: BaseSQLiteImplementation):
        """Test creating a table."""
        await impl.execute("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        # Table should exist - try inserting
        await impl.execute("INSERT INTO test (name) VALUES (?)", ("test_value",))
        await impl.commit()
        rows = await impl.execute("SELECT name FROM test")
        assert len(rows) == 1
        assert rows[0][0] == "test_value"

    async def test_insert_and_select(self, impl: BaseSQLiteImplementation):
        """Test inserting and selecting data."""
        await impl.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        await impl.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))
        await impl.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Bob", 25))
        await impl.commit()

        rows = await impl.execute("SELECT name, age FROM users ORDER BY name")
        assert len(rows) == 2
        assert rows[0] == ("Alice", 30)
        assert rows[1] == ("Bob", 25)

    async def test_update(self, impl: BaseSQLiteImplementation):
        """Test updating data."""
        await impl.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
        await impl.execute("INSERT INTO items (id, value) VALUES (?, ?)", (1, 100))
        await impl.commit()

        await impl.execute("UPDATE items SET value = ? WHERE id = ?", (200, 1))
        await impl.commit()

        rows = await impl.execute("SELECT value FROM items WHERE id = ?", (1,))
        assert len(rows) == 1
        assert rows[0][0] == 200

    async def test_delete(self, impl: BaseSQLiteImplementation):
        """Test deleting data."""
        await impl.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, data TEXT)")
        await impl.execute("INSERT INTO records (data) VALUES (?)", ("keep",))
        await impl.execute("INSERT INTO records (data) VALUES (?)", ("delete",))
        await impl.commit()

        await impl.execute("DELETE FROM records WHERE data = ?", ("delete",))
        await impl.commit()

        rows = await impl.execute("SELECT data FROM records")
        assert len(rows) == 1
        assert rows[0][0] == "keep"


class TestExecuteMany:
    """Tests for executemany operations."""

    async def test_batch_insert(self, impl: BaseSQLiteImplementation):
        """Test inserting multiple rows at once."""
        await impl.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")

        data = [(f"item_{i}",) for i in range(100)]
        await impl.executemany("INSERT INTO items (name) VALUES (?)", data)
        await impl.commit()

        rows = await impl.execute("SELECT COUNT(*) FROM items")
        assert rows[0][0] == 100

    async def test_batch_insert_with_multiple_columns(self, impl: BaseSQLiteImplementation):
        """Test batch insert with multiple columns."""
        await impl.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")

        data = [(f"product_{i}", 10.0 + i) for i in range(50)]
        await impl.executemany("INSERT INTO products (name, price) VALUES (?, ?)", data)
        await impl.commit()

        rows = await impl.execute("SELECT SUM(price) FROM products")
        expected_sum = sum(10.0 + i for i in range(50))
        assert abs(rows[0][0] - expected_sum) < 0.01


class TestTransactions:
    """Tests for transaction operations."""

    async def test_transaction_commit(self, impl: BaseSQLiteImplementation):
        """Test transaction commit."""
        await impl.execute("CREATE TABLE test (value INTEGER)")

        await impl.begin_transaction()
        await impl.execute("INSERT INTO test (value) VALUES (?)", (1,))
        await impl.execute("INSERT INTO test (value) VALUES (?)", (2,))
        await impl.commit()

        rows = await impl.execute("SELECT value FROM test ORDER BY value")
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    async def test_transaction_rollback(self, impl: BaseSQLiteImplementation):
        """Test transaction rollback."""
        await impl.execute("CREATE TABLE test (value INTEGER)")
        await impl.execute("INSERT INTO test (value) VALUES (?)", (1,))
        await impl.commit()

        await impl.begin_transaction()
        await impl.execute("INSERT INTO test (value) VALUES (?)", (2,))
        await impl.execute("INSERT INTO test (value) VALUES (?)", (3,))
        await impl.rollback()

        rows = await impl.execute("SELECT value FROM test")
        assert len(rows) == 1
        assert rows[0][0] == 1

    async def test_run_in_transaction_success(self, impl: BaseSQLiteImplementation):
        """Test run_in_transaction with successful operations."""
        await impl.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        await impl.execute("INSERT INTO accounts (id, balance) VALUES (?, ?)", (1, 100))
        await impl.execute("INSERT INTO accounts (id, balance) VALUES (?, ?)", (2, 50))
        await impl.commit()

        async def transfer(db: BaseSQLiteImplementation) -> None:
            await db.execute("UPDATE accounts SET balance = balance - 30 WHERE id = ?", (1,))
            await db.execute("UPDATE accounts SET balance = balance + 30 WHERE id = ?", (2,))

        await impl.run_in_transaction(transfer)

        rows = await impl.execute("SELECT id, balance FROM accounts ORDER BY id")
        assert rows[0] == (1, 70)
        assert rows[1] == (2, 80)

    async def test_run_in_transaction_failure(self, impl: BaseSQLiteImplementation):
        """Test run_in_transaction with failed operations."""
        await impl.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        await impl.execute("INSERT INTO accounts (id, balance) VALUES (?, ?)", (1, 100))
        await impl.commit()

        async def failing_transfer(db: BaseSQLiteImplementation) -> None:
            await db.execute("UPDATE accounts SET balance = balance - 30 WHERE id = ?", (1,))
            raise ValueError("Simulated error")

        with pytest.raises(ValueError):
            await impl.run_in_transaction(failing_transfer)

        rows = await impl.execute("SELECT balance FROM accounts WHERE id = ?", (1,))
        assert rows[0][0] == 100  # Rolled back


class TestDataTypes:
    """Tests for different data types."""

    async def test_integer_types(self, impl: BaseSQLiteImplementation):
        """Test integer values."""
        await impl.execute("CREATE TABLE numbers (value INTEGER)")
        test_values = [0, 1, -1, 2**31 - 1, -(2**31), 2**62]

        for val in test_values:
            await impl.execute("INSERT INTO numbers (value) VALUES (?)", (val,))
        await impl.commit()

        rows = await impl.execute("SELECT value FROM numbers ORDER BY value")
        retrieved = [r[0] for r in rows]
        assert sorted(retrieved) == sorted(test_values)

    async def test_text_types(self, impl: BaseSQLiteImplementation):
        """Test text values."""
        await impl.execute("CREATE TABLE texts (value TEXT)")
        test_values = ["", "hello", "hello world", "unicode: \u00e9\u00e0\u00fc", "emoji: 🎉"]

        for val in test_values:
            await impl.execute("INSERT INTO texts (value) VALUES (?)", (val,))
        await impl.commit()

        rows = await impl.execute("SELECT value FROM texts")
        retrieved = [r[0] for r in rows]
        assert set(retrieved) == set(test_values)

    async def test_real_types(self, impl: BaseSQLiteImplementation):
        """Test real (float) values."""
        await impl.execute("CREATE TABLE floats (value REAL)")
        test_values = [0.0, 1.5, -1.5, 3.14159, 1e-10, 1e10]

        for val in test_values:
            await impl.execute("INSERT INTO floats (value) VALUES (?)", (val,))
        await impl.commit()

        rows = await impl.execute("SELECT value FROM floats")
        retrieved = [r[0] for r in rows]
        for expected, actual in zip(sorted(test_values), sorted(retrieved)):
            assert abs(expected - actual) < 1e-5 or abs(expected - actual) / abs(expected) < 1e-5

    async def test_null_values(self, impl: BaseSQLiteImplementation):
        """Test NULL values."""
        await impl.execute("CREATE TABLE nullable (id INTEGER PRIMARY KEY, value TEXT)")
        await impl.execute("INSERT INTO nullable (id, value) VALUES (?, ?)", (1, None))
        await impl.execute("INSERT INTO nullable (id, value) VALUES (?, ?)", (2, "not null"))
        await impl.commit()

        rows = await impl.execute("SELECT id, value FROM nullable ORDER BY id")
        assert rows[0] == (1, None)
        assert rows[1] == (2, "not null")


class TestQueries:
    """Tests for various query patterns."""

    async def test_where_clause(self, impl: BaseSQLiteImplementation):
        """Test WHERE clause filtering."""
        await impl.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)")
        await impl.executemany(
            "INSERT INTO items (category, value) VALUES (?, ?)",
            [("A", 10), ("B", 20), ("A", 30), ("B", 40), ("A", 50)],
        )
        await impl.commit()

        rows = await impl.execute("SELECT value FROM items WHERE category = ? ORDER BY value", ("A",))
        assert len(rows) == 3
        assert [r[0] for r in rows] == [10, 30, 50]

    async def test_order_by(self, impl: BaseSQLiteImplementation):
        """Test ORDER BY clause."""
        await impl.execute("CREATE TABLE items (name TEXT, value INTEGER)")
        await impl.executemany(
            "INSERT INTO items (name, value) VALUES (?, ?)",
            [("c", 3), ("a", 1), ("b", 2)],
        )
        await impl.commit()

        rows = await impl.execute("SELECT name, value FROM items ORDER BY name")
        assert [r[0] for r in rows] == ["a", "b", "c"]

        rows = await impl.execute("SELECT name, value FROM items ORDER BY value DESC")
        assert [r[1] for r in rows] == [3, 2, 1]

    async def test_limit_offset(self, impl: BaseSQLiteImplementation):
        """Test LIMIT and OFFSET."""
        await impl.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        await impl.executemany("INSERT INTO items (id) VALUES (?)", [(i,) for i in range(10)])
        await impl.commit()

        rows = await impl.execute("SELECT id FROM items ORDER BY id LIMIT 3")
        assert [r[0] for r in rows] == [0, 1, 2]

        rows = await impl.execute("SELECT id FROM items ORDER BY id LIMIT 3 OFFSET 5")
        assert [r[0] for r in rows] == [5, 6, 7]

    async def test_aggregates(self, impl: BaseSQLiteImplementation):
        """Test aggregate functions."""
        await impl.execute("CREATE TABLE sales (amount REAL)")
        await impl.executemany(
            "INSERT INTO sales (amount) VALUES (?)",
            [(10.0,), (20.0,), (30.0,), (40.0,), (50.0,)],
        )
        await impl.commit()

        rows = await impl.execute("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales")
        assert rows[0][0] == 5  # COUNT
        assert rows[0][1] == 150.0  # SUM
        assert rows[0][2] == 30.0  # AVG
        assert rows[0][3] == 10.0  # MIN
        assert rows[0][4] == 50.0  # MAX

    async def test_group_by(self, impl: BaseSQLiteImplementation):
        """Test GROUP BY clause."""
        await impl.execute("CREATE TABLE orders (category TEXT, amount INTEGER)")
        await impl.executemany(
            "INSERT INTO orders (category, amount) VALUES (?, ?)",
            [("A", 10), ("B", 20), ("A", 15), ("B", 25), ("C", 30)],
        )
        await impl.commit()

        rows = await impl.execute(
            "SELECT category, SUM(amount) FROM orders GROUP BY category ORDER BY category"
        )
        assert rows[0] == ("A", 25)
        assert rows[1] == ("B", 45)
        assert rows[2] == ("C", 30)

    async def test_join(self, impl: BaseSQLiteImplementation):
        """Test JOIN operations."""
        await impl.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        await impl.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)")
        await impl.executemany(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob")],
        )
        await impl.executemany(
            "INSERT INTO orders (id, user_id, total) VALUES (?, ?, ?)",
            [(1, 1, 100.0), (2, 1, 50.0), (3, 2, 75.0)],
        )
        await impl.commit()

        rows = await impl.execute("""
            SELECT u.name, SUM(o.total) as total_spent
            FROM users u
            JOIN orders o ON u.id = o.user_id
            GROUP BY u.id
            ORDER BY u.name
        """)
        assert rows[0] == ("Alice", 150.0)
        assert rows[1] == ("Bob", 75.0)


class TestPragmas:
    """Tests for PRAGMA commands."""

    async def test_wal_mode(self, impl: BaseSQLiteImplementation):
        """Test that WAL mode is enabled."""
        rows = await impl.execute("PRAGMA journal_mode")
        assert rows[0][0].lower() == "wal"

    async def test_table_info(self, impl: BaseSQLiteImplementation):
        """Test PRAGMA table_info."""
        await impl.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL)")
        await impl.commit()

        rows = await impl.execute("PRAGMA table_info(test)")
        assert len(rows) == 3
        column_names = [r[1] for r in rows]
        assert "id" in column_names
        assert "name" in column_names
        assert "value" in column_names


class TestContextManager:
    """Tests for async context manager usage."""

    async def test_context_manager_closes(self, db_path):
        """Test that context manager properly closes connection."""
        async with MainThreadSQLite() as impl:
            await impl.setup(db_path)
            await impl.execute("CREATE TABLE test (id INTEGER)")
            await impl.commit()
        # Connection should be closed after exiting context


class TestLargeData:
    """Tests with larger data volumes."""

    async def test_large_batch_insert(self, impl: BaseSQLiteImplementation):
        """Test inserting a large number of rows."""
        await impl.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, data TEXT)")

        # Insert 1000 rows
        data = [(f"row_data_{i}" * 10,) for i in range(1000)]
        await impl.executemany("INSERT INTO large (data) VALUES (?)", data)
        await impl.commit()

        rows = await impl.execute("SELECT COUNT(*) FROM large")
        assert rows[0][0] == 1000

    async def test_large_text_field(self, impl: BaseSQLiteImplementation):
        """Test storing large text fields."""
        await impl.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)")

        # Insert a large text (100KB)
        large_content = "x" * 100_000
        await impl.execute("INSERT INTO documents (content) VALUES (?)", (large_content,))
        await impl.commit()

        rows = await impl.execute("SELECT LENGTH(content) FROM documents")
        assert rows[0][0] == 100_000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
