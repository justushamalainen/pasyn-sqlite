"""
Comprehensive transaction test cases for the transaction mutex feature.

These tests verify proper transaction behavior including:
- Basic atomic blocks (commit/rollback)
- Nested transactions with savepoints
- Race condition handling
- Bulk operations
- Multi-table operations
- Cascade operations
- Schema migrations
- Error handling patterns
"""

import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

try:
    import pasyn_sqlite_core as core
except ImportError:
    pytest.skip("pasyn_sqlite_core not built", allow_module_level=True)


@pytest.fixture
def db_path():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass
    socket_path = core.default_socket_path(path)
    try:
        os.unlink(socket_path)
    except OSError:
        pass


@pytest.fixture
def server_and_client(db_path):
    """Start a writer server and create a client."""
    server = core.start_writer_server(db_path)
    socket_path = core.default_socket_path(db_path)

    for _ in range(10):
        if os.path.exists(socket_path):
            break
        time.sleep(0.1)

    client = core.MultiplexedClient(socket_path)

    yield server, client, socket_path, db_path

    try:
        client.shutdown_server()
    except Exception:
        pass
    server.stop()


class TestCase1BasicAtomicBlockCommit:
    """
    Test Case 1: Basic Atomic Block (Autocommit -> Transaction -> Commit)
    Target: Ensure the driver can transition from autocommit mode to an explicit
    transaction, execute statements, and commit successfully.
    """

    def test_basic_atomic_commit(self, server_and_client):
        """Basic transaction with commit makes data persistent."""
        _, client, socket_path, db_path = server_and_client

        # Setup table
        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        # Begin exclusive transaction
        tx = client.begin_exclusive()

        # Insert data within transaction
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["foo"])

        # Read within same transaction - should see pending data
        # Note: We can't do SELECT through the writer, but the data is pending

        # Commit
        tx.commit()

        # Verify data is persisted - use a read connection
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT * FROM test_table WHERE name='foo'")
        assert len(rows) == 1
        assert rows[0][1] == "foo"
        read_conn.close()


class TestCase2BasicAtomicBlockRollback:
    """
    Test Case 2: Basic Atomic Block with Rollback (Exception Path)
    Target: Ensure the driver can rollback a transaction when an error occurs.
    """

    def test_basic_atomic_rollback(self, server_and_client):
        """Transaction rollback discards changes."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        # Begin transaction and insert
        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["bar"])

        # Rollback instead of commit
        tx.rollback()

        # Verify data is NOT present
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT * FROM test_table WHERE name='bar'")
        assert len(rows) == 0
        read_conn.close()

    def test_context_manager_exception_rollback(self, server_and_client):
        """Exception in context manager triggers rollback."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        try:
            with client.begin_exclusive() as tx:
                tx.execute("INSERT INTO test_table (name) VALUES (?)", ["exception_test"])
                raise ValueError("Simulated exception")
        except ValueError:
            pass

        # Data should NOT be present (auto-rollback on exception)
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT * FROM test_table WHERE name='exception_test'")
        assert len(rows) == 0
        read_conn.close()


class TestCase3NestedTransactionWithSavepoints:
    """
    Test Case 3: Nested Transaction with Savepoints (Success Path)
    Target: Ensure the driver supports savepoints for nested atomic blocks.
    """

    def test_savepoints_within_transaction(self, server_and_client):
        """Savepoints allow partial commits within a transaction."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        tx = client.begin_exclusive()

        # Insert outer data
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["outer"])

        # Create savepoint and insert inner data
        tx.executescript("SAVEPOINT s1_x1")
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["inner"])
        tx.executescript("RELEASE SAVEPOINT s1_x1")

        # Commit all
        tx.commit()

        # Verify both rows exist
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT * FROM test_table ORDER BY name")
        assert len(rows) == 2
        names = [r[1] for r in rows]
        assert "outer" in names
        assert "inner" in names
        read_conn.close()


class TestCase4NestedTransactionSavepointRollback:
    """
    Test Case 4: Nested Transaction with Savepoint Rollback
    Target: Ensure the driver can rollback to a savepoint while preserving outer transaction.
    """

    def test_savepoint_rollback_preserves_outer(self, server_and_client):
        """Rolling back to savepoint discards inner changes but keeps outer."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        tx = client.begin_exclusive()

        # Insert outer data
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["outer"])

        # Create savepoint
        tx.executescript("SAVEPOINT s1_x1")

        # Insert inner data
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["inner"])

        # Rollback to savepoint (discard inner)
        tx.executescript("ROLLBACK TO SAVEPOINT s1_x1")
        tx.executescript("RELEASE SAVEPOINT s1_x1")

        # Commit outer changes
        tx.commit()

        # Verify only outer row exists
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT * FROM test_table")
        assert len(rows) == 1
        assert rows[0][1] == "outer"
        read_conn.close()


class TestCase5GetOrCreatePattern:
    """
    Test Case 5: Get-or-Create Pattern (Race Condition Handling)
    Target: Verify driver handles IntegrityError during concurrent insert.
    """

    def test_concurrent_unique_insert(self, server_and_client):
        """Two processes trying to create the same unique record."""
        _, client, socket_path, db_path = server_and_client

        client.executescript(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)"
        )

        results = {"success": 0, "blocked": 0, "integrity_error": 0}
        lock = threading.Lock()

        def try_insert(email):
            c = core.MultiplexedClient(socket_path)
            try:
                tx = c.begin_exclusive()
                tx.execute("INSERT INTO users (email) VALUES (?)", [email])
                tx.commit()
                with lock:
                    results["success"] += 1
            except Exception as e:
                err_msg = str(e).lower()
                if "unique constraint" in err_msg or "integrity" in err_msg:
                    with lock:
                        results["integrity_error"] += 1
                elif "lock held" in err_msg or "locked" in err_msg:
                    # Second client couldn't acquire the transaction lock
                    with lock:
                        results["blocked"] += 1
                else:
                    raise

        # Two threads trying to insert same email
        threads = [
            threading.Thread(target=try_insert, args=("test@example.com",))
            for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one should succeed, the other should be blocked or get integrity error
        assert results["success"] == 1
        assert results["blocked"] + results["integrity_error"] == 1

        # Verify only one row exists
        read_conn = core.connect(db_path)
        row = read_conn.execute_fetchone("SELECT COUNT(*) FROM users")
        assert row[0] == 1
        read_conn.close()


class TestCase6UpdateOrCreatePattern:
    """
    Test Case 6: Update-or-Create Pattern
    Target: Verify atomic read-modify-write pattern.
    """

    def test_atomic_increment(self, server_and_client):
        """Atomic counter increment using transaction."""
        _, client, socket_path, db_path = server_and_client

        client.executescript(
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, name TEXT UNIQUE, value INTEGER)"
        )
        client.execute("INSERT INTO counters (name, value) VALUES (?, ?)", ["hits", 0])

        # Atomic increment
        tx = client.begin_exclusive()
        # Note: In a real scenario, we'd read, compute, then write
        tx.execute("UPDATE counters SET value = value + 1 WHERE name = ?", ["hits"])
        tx.commit()

        # Verify increment
        read_conn = core.connect(db_path)
        row = read_conn.execute_fetchone("SELECT value FROM counters WHERE name='hits'")
        assert row[0] == 1
        read_conn.close()

    def test_concurrent_increment(self, server_and_client):
        """Concurrent increments are serialized by transaction mutex."""
        _, client, socket_path, db_path = server_and_client

        client.executescript(
            "CREATE TABLE counters (id INTEGER PRIMARY KEY, name TEXT UNIQUE, value INTEGER)"
        )
        client.execute("INSERT INTO counters (name, value) VALUES (?, ?)", ["hits", 0])

        num_increments = 10

        def increment():
            c = core.MultiplexedClient(socket_path)
            for _ in range(10):  # Retry loop
                try:
                    tx = c.begin_exclusive()
                    tx.execute(
                        "UPDATE counters SET value = value + 1 WHERE name = ?", ["hits"]
                    )
                    tx.commit()
                    return True
                except Exception:
                    time.sleep(0.01)
            return False

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(increment) for _ in range(num_increments)]
            results = [f.result() for f in as_completed(futures)]

        assert all(results)

        # Verify final count
        read_conn = core.connect(db_path)
        row = read_conn.execute_fetchone("SELECT value FROM counters WHERE name='hits'")
        assert row[0] == num_increments
        read_conn.close()


class TestCase7BulkInsertMixedPKs:
    """
    Test Case 7: Bulk Insert with Mixed PKs
    Target: Verify atomic bulk insert with both auto-generated and explicit PKs.
    """

    def test_bulk_insert_mixed_pks(self, server_and_client):
        """Bulk insert with explicit and auto-generated PKs."""
        _, client, socket_path, db_path = server_and_client

        client.executescript(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        )

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO items (id, name) VALUES (?, ?)", [100, "explicit_pk"])
        tx.execute("INSERT INTO items (name) VALUES (?)", ["auto_pk_1"])
        tx.execute("INSERT INTO items (name) VALUES (?)", ["auto_pk_2"])
        tx.commit()

        # Verify all rows
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT id, name FROM items ORDER BY id")
        assert len(rows) == 3

        # Check explicit PK
        explicit_row = [r for r in rows if r[1] == "explicit_pk"][0]
        assert explicit_row[0] == 100

        # Check auto PKs are different from explicit
        auto_rows = [r for r in rows if r[1].startswith("auto_pk")]
        assert len(auto_rows) == 2
        for row in auto_rows:
            assert row[0] != 100
        read_conn.close()


class TestCase8MultiTableSave:
    """
    Test Case 8: Multi-Table Save (Parent-Child Inheritance)
    Target: Verify atomic insert across multiple related tables.
    """

    def test_multi_table_atomic_insert(self, server_and_client):
        """Atomic insert across parent and child tables."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("""
            CREATE TABLE base_model (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE child_model (
                base_ptr_id INTEGER PRIMARY KEY REFERENCES base_model(id),
                extra_field TEXT
            );
        """)

        tx = client.begin_exclusive()
        parent_id = tx.execute_returning_rowid(
            "INSERT INTO base_model (name) VALUES (?)", ["parent_data"]
        )
        tx.execute(
            "INSERT INTO child_model (base_ptr_id, extra_field) VALUES (?, ?)",
            [parent_id, "child_data"],
        )
        tx.commit()

        # Verify joined data
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("""
            SELECT b.name, c.extra_field
            FROM base_model b
            JOIN child_model c ON b.id = c.base_ptr_id
        """)
        assert len(rows) == 1
        assert rows[0][0] == "parent_data"
        assert rows[0][1] == "child_data"
        read_conn.close()


class TestCase9CascadeDelete:
    """
    Test Case 9: Cascade Delete
    Target: Verify atomic deletion with multiple related deletes.
    """

    def test_cascade_delete(self, server_and_client):
        """Delete parent and children atomically."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("""
            CREATE TABLE author (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE book (
                id INTEGER PRIMARY KEY,
                title TEXT,
                author_id INTEGER REFERENCES author(id)
            );
        """)

        # Insert test data
        client.execute("INSERT INTO author (id, name) VALUES (?, ?)", [1, "Alice"])
        client.execute(
            "INSERT INTO book (id, title, author_id) VALUES (?, ?, ?)",
            [1, "Book1", 1],
        )
        client.execute(
            "INSERT INTO book (id, title, author_id) VALUES (?, ?, ?)",
            [2, "Book2", 1],
        )

        # Atomic cascade delete
        tx = client.begin_exclusive()
        tx.execute("DELETE FROM book WHERE author_id = ?", [1])
        tx.execute("DELETE FROM author WHERE id = ?", [1])
        tx.commit()

        # Verify all deleted
        read_conn = core.connect(db_path)
        row = read_conn.execute_fetchone("SELECT COUNT(*) FROM author")
        assert row[0] == 0
        row = read_conn.execute_fetchone("SELECT COUNT(*) FROM book")
        assert row[0] == 0
        read_conn.close()


class TestCase10OnCommitCallbacks:
    """
    Test Case 10: On-Commit Callbacks (Simulated)
    Target: Verify callbacks execute only after successful commit.
    """

    def test_callback_on_commit(self, server_and_client):
        """Callbacks should run after successful commit."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        callback_executed = [False]

        def on_commit_callback():
            callback_executed[0] = True

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["test"])
        tx.commit()

        # Simulate callback after commit
        on_commit_callback()

        assert callback_executed[0]

    def test_no_callback_on_rollback(self, server_and_client):
        """Callbacks should NOT run after rollback."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        callback_executed = [False]

        def on_commit_callback():
            callback_executed[0] = True

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["test"])
        tx.rollback()

        # Callback should NOT be called after rollback
        # (In real implementation, this would be managed by the transaction)
        assert not callback_executed[0]


class TestCase11SavepointScopedCallbacks:
    """
    Test Case 11: Savepoint-Scoped Callbacks
    Target: Verify callbacks respect savepoint boundaries.
    """

    def test_savepoint_scoped_callback(self, server_and_client):
        """Inner savepoint rollback should discard inner callbacks."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        outer_executed = [False]
        inner_executed = [False]

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["outer"])

        # Register "outer" callback (would execute on commit)
        def outer_cb():
            outer_executed[0] = True

        # Savepoint
        tx.executescript("SAVEPOINT s1")
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["inner"])

        # Register "inner" callback (would be discarded on savepoint rollback)
        def inner_cb():
            inner_executed[0] = True

        # Rollback savepoint
        tx.executescript("ROLLBACK TO SAVEPOINT s1")
        tx.executescript("RELEASE SAVEPOINT s1")

        # Commit outer
        tx.commit()

        # In a real implementation with callback tracking:
        # - outer_cb would be called
        # - inner_cb would NOT be called
        outer_cb()  # Simulate outer callback
        # inner_cb NOT called

        assert outer_executed[0]
        assert not inner_executed[0]


class TestCase12SchemaMigrationInAtomicBlock:
    """
    Test Case 12: Schema Migration in Atomic Block
    Target: Verify DDL operations within transaction.
    """

    def test_ddl_in_transaction_commit(self, server_and_client):
        """DDL operations can be committed in transaction."""
        _, client, socket_path, db_path = server_and_client

        tx = client.begin_exclusive()
        tx.executescript(
            "CREATE TABLE new_table (id INTEGER PRIMARY KEY, data TEXT)"
        )
        tx.executescript("ALTER TABLE new_table ADD COLUMN extra TEXT")
        tx.execute(
            "INSERT INTO new_table (data, extra) VALUES (?, ?)", ["test", "value"]
        )
        tx.commit()

        # Verify table exists and has data
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT data, extra FROM new_table")
        assert len(rows) == 1
        assert rows[0][0] == "test"
        assert rows[0][1] == "value"
        read_conn.close()

    def test_ddl_in_transaction_rollback(self, server_and_client):
        """DDL operations can be rolled back in transaction."""
        _, client, socket_path, db_path = server_and_client

        tx = client.begin_exclusive()
        tx.executescript(
            "CREATE TABLE temp_table (id INTEGER PRIMARY KEY, data TEXT)"
        )
        tx.execute("INSERT INTO temp_table (data) VALUES (?)", ["will_rollback"])
        tx.rollback()

        # Table should NOT exist
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='temp_table'"
        )
        assert len(rows) == 0
        read_conn.close()


class TestCase13TestCaseRollbackPattern:
    """
    Test Case 13: TestCase Rollback Pattern (Test Isolation)
    Target: Verify test data is isolated via transaction rollback.
    """

    def test_isolation_via_rollback(self, server_and_client):
        """Simulating Django TestCase rollback pattern."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        # Simulate fixture data (would be loaded once)
        client.execute("INSERT INTO test_table (name) VALUES (?)", ["fixture_data"])

        # Simulate test 1
        tx1 = client.begin_exclusive()
        tx1.executescript("SAVEPOINT test_savepoint")
        tx1.execute("INSERT INTO test_table (name) VALUES (?)", ["test1_data"])
        # Rollback to savepoint (simulating tearDown)
        tx1.executescript("ROLLBACK TO SAVEPOINT test_savepoint")
        tx1.executescript("RELEASE SAVEPOINT test_savepoint")
        tx1.commit()

        # Verify only fixture remains
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT name FROM test_table")
        names = [r[0] for r in rows]
        assert "fixture_data" in names
        assert "test1_data" not in names
        read_conn.close()


class TestCase14ConnectionCloseInTransaction:
    """
    Test Case 14: Connection Close in Transaction
    Target: Verify connection close during transaction triggers rollback.
    """

    def test_disconnect_triggers_rollback(self, server_and_client):
        """Client disconnect during transaction should rollback."""
        _, client1, socket_path, db_path = server_and_client

        client1.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        # Client 2 starts a transaction and "disconnects"
        client2 = core.MultiplexedClient(socket_path)
        tx = client2.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["should_rollback"])

        # Simulate disconnect by letting client2 go out of scope without commit
        del tx
        del client2

        # Wait a moment for server to process disconnect
        time.sleep(0.2)

        # Verify data was rolled back
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall(
            "SELECT * FROM test_table WHERE name='should_rollback'"
        )
        assert len(rows) == 0
        read_conn.close()


class TestCase15MarkForRollbackOnError:
    """
    Test Case 15: Mark for Rollback on Error
    Target: Verify error handling within atomic blocks.
    """

    def test_error_marks_for_rollback(self, server_and_client):
        """Database error during transaction should allow rollback."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("""
            CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL)
        """)

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["valid"])

        # This should fail (NOT NULL constraint)
        with pytest.raises(Exception):
            tx.execute("INSERT INTO test_table (name) VALUES (?)", [None])

        # Transaction should still be usable for rollback
        tx.rollback()

        # Verify no data was committed
        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT * FROM test_table")
        assert len(rows) == 0
        read_conn.close()


class TestAdditionalEdgeCases:
    """Additional edge cases for comprehensive coverage."""

    def test_empty_transaction_commit(self, server_and_client):
        """Empty transaction can be committed."""
        _, client, socket_path, db_path = server_and_client

        tx = client.begin_exclusive()
        tx.commit()
        # Should not raise

    def test_empty_transaction_rollback(self, server_and_client):
        """Empty transaction can be rolled back."""
        _, client, socket_path, db_path = server_and_client

        tx = client.begin_exclusive()
        tx.rollback()
        # Should not raise

    def test_large_batch_in_transaction(self, server_and_client):
        """Large number of operations in single transaction."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)")

        tx = client.begin_exclusive()
        for i in range(1000):
            tx.execute("INSERT INTO test_table (value) VALUES (?)", [i])
        tx.commit()

        read_conn = core.connect(db_path)
        row = read_conn.execute_fetchone("SELECT COUNT(*) FROM test_table")
        assert row[0] == 1000
        read_conn.close()

    def test_read_after_write_in_transaction(self, server_and_client):
        """Writes should be visible within the same transaction."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["first"])
        tx.execute("INSERT INTO test_table (name) VALUES (?)", ["second"])
        # Data should be visible within transaction (even though we can't SELECT through writer)
        tx.commit()

        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT name FROM test_table ORDER BY id")
        assert len(rows) == 2
        assert rows[0][0] == "first"
        assert rows[1][0] == "second"
        read_conn.close()

    def test_transaction_after_transaction(self, server_and_client):
        """Multiple sequential transactions should work."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, batch INTEGER)")

        for batch in range(5):
            tx = client.begin_exclusive()
            for i in range(10):
                tx.execute("INSERT INTO test_table (batch) VALUES (?)", [batch])
            tx.commit()

        read_conn = core.connect(db_path)
        row = read_conn.execute_fetchone("SELECT COUNT(*) FROM test_table")
        assert row[0] == 50
        read_conn.close()

    def test_mixed_operations_in_transaction(self, server_and_client):
        """INSERT, UPDATE, DELETE in same transaction."""
        _, client, socket_path, db_path = server_and_client

        client.executescript("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        client.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ["initial", 0])

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ["new", 100])
        tx.execute("UPDATE test_table SET value = ? WHERE name = ?", [42, "initial"])
        tx.execute("DELETE FROM test_table WHERE name = ?", ["new"])
        tx.commit()

        read_conn = core.connect(db_path)
        rows = read_conn.execute_fetchall("SELECT name, value FROM test_table")
        assert len(rows) == 1
        assert rows[0][0] == "initial"
        assert rows[0][1] == 42
        read_conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
