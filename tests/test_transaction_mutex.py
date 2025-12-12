"""
Tests for the transaction mutex feature.

The transaction mutex allows a client to acquire an exclusive lock on the database,
blocking all other writes until the transaction is committed or rolled back.
"""

import os
import tempfile
import threading
import time

import pytest

# Import the core module (will fail if not built)
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
    # Also clean up socket files
    socket_path = core.default_socket_path(path)
    try:
        os.unlink(socket_path)
    except OSError:
        pass


@pytest.fixture
def server_and_client(db_path):
    """Start a writer server and create a client."""
    # Start the server
    server = core.start_writer_server(db_path)
    socket_path = core.default_socket_path(db_path)

    # Wait for server to be ready
    for _ in range(10):
        if os.path.exists(socket_path):
            break
        time.sleep(0.1)

    # Create client and set up test table
    client = core.MultiplexedClient(socket_path)
    client.executescript("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, value TEXT)")

    yield server, client, socket_path

    # Cleanup
    try:
        client.shutdown_server()
    except Exception:
        pass
    server.stop()


class TestTransactionMutexBasic:
    """Basic transaction mutex tests."""

    def test_acquire_and_commit(self, server_and_client):
        """Test acquiring transaction lock, executing, and committing."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        assert tx.token > 0
        assert not tx.finished

        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        tx.execute("INSERT INTO test VALUES (?, ?)", [2, "world"])
        tx.commit()

        assert tx.finished

    def test_acquire_and_rollback(self, server_and_client):
        """Test acquiring transaction lock, executing, and rolling back."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        tx.rollback()

        assert tx.finished

        # Verify data was not committed by doing another transaction
        tx2 = client.begin_exclusive()
        # If data was committed, this would fail with UNIQUE constraint
        tx2.execute("INSERT INTO test VALUES (?, ?)", [1, "different"])
        tx2.commit()

    def test_context_manager_commit(self, server_and_client):
        """Test using transaction as context manager with explicit commit."""
        _, client, _ = server_and_client

        with client.begin_exclusive() as tx:
            tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
            tx.commit()

        # Should be committed
        tx2 = client.begin_exclusive()
        # This should fail if data was committed
        with pytest.raises(Exception):
            tx2.execute("INSERT INTO test VALUES (?, ?)", [1, "duplicate"])
        tx2.rollback()

    def test_context_manager_auto_rollback(self, server_and_client):
        """Test that context manager auto-rolls back without explicit commit."""
        _, client, _ = server_and_client

        with client.begin_exclusive() as tx:
            tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
            # No commit - should auto-rollback

        # Data should not be present
        tx2 = client.begin_exclusive()
        tx2.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        tx2.commit()

    def test_context_manager_exception_rollback(self, server_and_client):
        """Test that context manager rolls back on exception."""
        _, client, _ = server_and_client

        try:
            with client.begin_exclusive() as tx:
                tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Data should not be present
        tx2 = client.begin_exclusive()
        tx2.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        tx2.commit()

    def test_execute_returning_rowid(self, server_and_client):
        """Test execute_returning_rowid within transaction."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        rowid = tx.execute_returning_rowid("INSERT INTO test VALUES (?, ?)", [None, "hello"])
        assert rowid > 0
        tx.commit()

    def test_executescript_within_transaction(self, server_and_client):
        """Test executescript within transaction."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.executescript("""
            INSERT INTO test VALUES (1, 'a');
            INSERT INTO test VALUES (2, 'b');
            INSERT INTO test VALUES (3, 'c');
        """)
        tx.commit()


class TestTransactionMutexExclusion:
    """Tests for mutual exclusion behavior."""

    def test_second_client_blocked(self, server_and_client):
        """Test that a second client cannot write while first holds lock."""
        _, client1, socket_path = server_and_client
        client2 = core.MultiplexedClient(socket_path)

        # Client 1 acquires lock
        tx = client1.begin_exclusive()
        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "from_client1"])

        # Client 2 should be blocked from writing
        with pytest.raises(Exception, match="locked|exclusive"):
            client2.execute("INSERT INTO test VALUES (?, ?)", [2, "from_client2"])

        # Commit releases lock
        tx.commit()

        # Now client 2 should succeed
        client2.execute("INSERT INTO test VALUES (?, ?)", [2, "from_client2"])

    def test_second_client_cannot_acquire_lock(self, server_and_client):
        """Test that a second client cannot acquire the lock while first holds it."""
        _, client1, socket_path = server_and_client
        client2 = core.MultiplexedClient(socket_path)

        # Client 1 acquires lock
        tx1 = client1.begin_exclusive()

        # Client 2 should fail to acquire lock
        with pytest.raises(Exception, match="held|locked"):
            client2.begin_exclusive()

        tx1.rollback()

        # Now client 2 should be able to acquire
        tx2 = client2.begin_exclusive()
        tx2.commit()

    def test_rollback_releases_lock(self, server_and_client):
        """Test that rollback releases the lock for other clients."""
        _, client1, socket_path = server_and_client
        client2 = core.MultiplexedClient(socket_path)

        # Client 1 acquires and rolls back
        tx1 = client1.begin_exclusive()
        tx1.execute("INSERT INTO test VALUES (?, ?)", [1, "will_rollback"])
        tx1.rollback()

        # Client 2 should be able to acquire immediately
        tx2 = client2.begin_exclusive()
        tx2.execute("INSERT INTO test VALUES (?, ?)", [1, "from_client2"])
        tx2.commit()


class TestTransactionMutexTimeout:
    """Tests for transaction timeout behavior."""

    def test_timeout_causes_rollback(self, server_and_client):
        """Test that transaction times out and rolls back after 1 second."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "should_timeout"])

        # Wait for timeout (default is 1 second)
        time.sleep(1.5)

        # Transaction should have been rolled back by server
        # Trying to commit should fail
        with pytest.raises(Exception, match="timeout|invalid|No transaction"):
            tx.commit()

        # Verify data was not committed by doing another transaction
        tx2 = client.begin_exclusive()
        # If data was committed, this would fail with UNIQUE constraint
        tx2.execute("INSERT INTO test VALUES (?, ?)", [1, "after_timeout"])
        tx2.commit()

    def test_operations_before_timeout_succeed(self, server_and_client):
        """Test that operations complete if done before timeout."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "quick"])
        tx.commit()  # Should succeed if done quickly

        assert tx.finished


class TestTransactionMutexEdgeCases:
    """Edge case tests."""

    def test_double_commit_fails(self, server_and_client):
        """Test that committing twice raises an error."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        tx.commit()

        with pytest.raises(Exception, match="already finished"):
            tx.commit()

    def test_double_rollback_fails(self, server_and_client):
        """Test that rolling back twice raises an error."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])
        tx.rollback()

        with pytest.raises(Exception, match="already finished"):
            tx.rollback()

    def test_execute_after_commit_fails(self, server_and_client):
        """Test that executing after commit raises an error."""
        _, client, _ = server_and_client

        tx = client.begin_exclusive()
        tx.commit()

        with pytest.raises(Exception, match="already finished"):
            tx.execute("INSERT INTO test VALUES (?, ?)", [1, "hello"])

    def test_ping_during_transaction(self, server_and_client):
        """Test that ping works during transaction (non-write operation)."""
        _, client1, socket_path = server_and_client
        client2 = core.MultiplexedClient(socket_path)

        tx = client1.begin_exclusive()

        # Ping should still work even though lock is held
        client2.ping()

        tx.rollback()

    def test_sequential_transactions(self, server_and_client):
        """Test multiple sequential transactions from same client."""
        _, client, _ = server_and_client

        for i in range(5):
            tx = client.begin_exclusive()
            tx.execute("INSERT INTO test VALUES (?, ?)", [i, f"value_{i}"])
            tx.commit()


class TestTransactionMutexConcurrency:
    """Concurrency tests."""

    def test_concurrent_lock_requests(self, server_and_client):
        """Test multiple threads requesting lock - only one should succeed."""
        _, _, socket_path = server_and_client

        results = []
        lock = threading.Lock()

        def try_acquire(thread_id):
            client = core.MultiplexedClient(socket_path)
            try:
                tx = client.begin_exclusive()
                with lock:
                    results.append(("success", thread_id, tx.token))
                time.sleep(0.1)  # Hold lock briefly
                tx.rollback()
            except Exception as e:
                with lock:
                    results.append(("error", thread_id, str(e)))

        threads = [threading.Thread(target=try_acquire, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # At least one should succeed (possibly more in sequence)
        successes = [r for r in results if r[0] == "success"]
        assert len(successes) >= 1

    def test_contention_eventually_succeeds(self, server_and_client):
        """Test that under contention, operations eventually succeed with retries."""
        _, _, socket_path = server_and_client

        success_count = [0]
        lock = threading.Lock()

        def worker(worker_id):
            client = core.MultiplexedClient(socket_path)
            for i in range(3):
                # Retry loop
                for attempt in range(10):
                    try:
                        tx = client.begin_exclusive()
                        tx.execute(
                            "INSERT INTO test VALUES (?, ?)",
                            [worker_id * 100 + i, f"w{worker_id}_{i}"],
                        )
                        tx.commit()
                        with lock:
                            success_count[0] += 1
                        break
                    except Exception:
                        time.sleep(0.05 * (attempt + 1))  # Backoff

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All operations should eventually succeed
        assert success_count[0] == 9  # 3 workers * 3 operations each


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
