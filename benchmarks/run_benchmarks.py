#!/usr/bin/env python3
"""
Performance benchmarks comparing SQLite implementations.

Compares:
1. Main thread (synchronous) - baseline
2. Single DB thread - all operations in one background thread
3. Pasyn-sqlite - single writer + multiple readers with work stealing

Benchmark categories:
- Small reads (single row)
- Large reads (1000+ rows)
- Multi-table reads (joins)
- Small writes (single row inserts)
- Large writes (single row with large data)
- Batch writes (many rows at once)
- Mixed workloads (concurrent reads and writes)
"""

from __future__ import annotations

import asyncio
import os
import statistics
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Coroutine, Any

from implementations import (
    BaseSQLiteImplementation,
    MainThreadSQLite,
    SingleThreadSQLite,
    PasynPoolSQLite,
)


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    implementation: str
    iterations: int
    total_time_ms: float
    min_time_ms: float
    max_time_ms: float
    mean_time_ms: float
    median_time_ms: float
    std_dev_ms: float
    times_ms: list[float] = field(default_factory=list, repr=False)

    @classmethod
    def from_times(
        cls, name: str, implementation: str, times_ms: list[float]
    ) -> "BenchmarkResult":
        return cls(
            name=name,
            implementation=implementation,
            iterations=len(times_ms),
            total_time_ms=sum(times_ms),
            min_time_ms=min(times_ms),
            max_time_ms=max(times_ms),
            mean_time_ms=statistics.mean(times_ms),
            median_time_ms=statistics.median(times_ms),
            std_dev_ms=statistics.stdev(times_ms) if len(times_ms) > 1 else 0,
            times_ms=times_ms,
        )


@dataclass
class TaskResult:
    """Result of a task within a workload."""

    task_id: int
    start_time: float
    end_time: float
    duration_ms: float


@dataclass
class WorkloadResult:
    """Result of a workload benchmark."""

    name: str
    implementation: str
    total_time_ms: float
    task_results: list[TaskResult]
    tasks_per_second: float
    mean_task_time_ms: float
    median_task_time_ms: float


class Benchmark:
    """Benchmark runner."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.results: list[BenchmarkResult | WorkloadResult] = []

    async def setup_schema(self, impl: BaseSQLiteImplementation) -> None:
        """Create the test schema."""
        # Users table (small records)
        await impl.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Products table (for joins)
        await impl.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT,
                description TEXT
            )
        """)

        # Orders table (for joins)
        await impl.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER,
                order_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Large data table
        await impl.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                metadata TEXT
            )
        """)

        await impl.commit()

    async def populate_test_data(self, impl: BaseSQLiteImplementation) -> None:
        """Populate tables with test data."""
        # Insert users
        users = [(f"User {i}", f"user{i}@example.com", 20 + (i % 50)) for i in range(1000)]
        await impl.executemany(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)", users
        )

        # Insert products
        products = [
            (f"Product {i}", 10.0 + (i % 100), f"Category {i % 10}", f"Description for product {i}")
            for i in range(500)
        ]
        await impl.executemany(
            "INSERT INTO products (name, price, category, description) VALUES (?, ?, ?, ?)",
            products,
        )

        # Insert orders
        orders = [
            ((i % 1000) + 1, (i % 500) + 1, 1 + (i % 10))
            for i in range(5000)
        ]
        await impl.executemany(
            "INSERT INTO orders (user_id, product_id, quantity) VALUES (?, ?, ?)",
            orders,
        )

        # Insert documents with larger content
        documents = [
            (f"Document {i}", "x" * 1000, f'{{"key": "value{i}"}}')
            for i in range(500)
        ]
        await impl.executemany(
            "INSERT INTO documents (title, content, metadata) VALUES (?, ?, ?)",
            documents,
        )

        await impl.commit()

    async def run_timed(
        self,
        func: Callable[[], Coroutine[Any, Any, Any]],
        iterations: int = 500,
    ) -> list[float]:
        """Run a function multiple times and return timing results."""
        times: list[float] = []
        for _ in range(iterations):
            start = time.perf_counter()
            await func()
            end = time.perf_counter()
            times.append((end - start) * 1000)  # Convert to ms
        return times

    # ==================== Read Benchmarks ====================

    async def bench_small_read(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Read single row by primary key."""

        async def task() -> None:
            await impl.execute("SELECT * FROM users WHERE id = ?", (500,))

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("small_read", impl_name, times)

    async def bench_large_read(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Read many rows."""

        async def task() -> None:
            await impl.execute("SELECT * FROM users WHERE age > ?", (30,))

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("large_read", impl_name, times)

    async def bench_large_read_ordered(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Read many rows with ORDER BY."""

        async def task() -> None:
            await impl.execute(
                "SELECT * FROM users WHERE age > ? ORDER BY name", (25,)
            )

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("large_read_ordered", impl_name, times)

    async def bench_join_small(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Simple two-table join."""

        async def task() -> None:
            await impl.execute("""
                SELECT o.id, u.name, p.name, o.quantity
                FROM orders o
                JOIN users u ON o.user_id = u.id
                JOIN products p ON o.product_id = p.id
                WHERE o.id = ?
            """, (100,))

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("join_small", impl_name, times)

    async def bench_join_large(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Join returning many rows."""

        async def task() -> None:
            await impl.execute("""
                SELECT o.id, u.name, p.name, o.quantity
                FROM orders o
                JOIN users u ON o.user_id = u.id
                JOIN products p ON o.product_id = p.id
                WHERE u.age > ?
                LIMIT 100
            """, (40,))

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("join_large", impl_name, times)

    async def bench_aggregate(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Aggregate query with GROUP BY."""

        async def task() -> None:
            await impl.execute("""
                SELECT u.age, COUNT(*) as order_count, SUM(o.quantity) as total_qty
                FROM orders o
                JOIN users u ON o.user_id = u.id
                GROUP BY u.age
            """)

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("aggregate", impl_name, times)

    # ==================== Write Benchmarks ====================

    async def bench_small_write(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Single row insert."""
        counter = [0]

        async def task() -> None:
            counter[0] += 1
            await impl.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                (f"BenchUser{counter[0]}", f"bench{counter[0]}@test.com", 25),
            )
            await impl.commit()

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("small_write", impl_name, times)

    async def bench_large_write(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Single row insert with large data."""
        counter = [0]
        large_content = "x" * 10000  # 10KB of data

        async def task() -> None:
            counter[0] += 1
            await impl.execute(
                "INSERT INTO documents (title, content, metadata) VALUES (?, ?, ?)",
                (f"BenchDoc{counter[0]}", large_content, '{"bench": true}'),
            )
            await impl.commit()

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("large_write", impl_name, times)

    async def bench_batch_write(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Batch insert (100 rows at once)."""
        counter = [0]

        async def task() -> None:
            base = counter[0] * 100
            counter[0] += 1
            rows = [
                (f"BatchUser{base + i}", f"batch{base + i}@test.com", 30)
                for i in range(100)
            ]
            await impl.executemany(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)", rows
            )
            await impl.commit()

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("batch_write", impl_name, times)

    async def bench_update(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Update single row."""

        async def task() -> None:
            await impl.execute(
                "UPDATE users SET age = age + 1 WHERE id = ?", (100,)
            )
            await impl.commit()

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("update", impl_name, times)

    async def bench_delete(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Delete and reinsert (to maintain data)."""

        async def task() -> None:
            # Delete
            await impl.execute("DELETE FROM users WHERE id = ?", (999,))
            # Reinsert
            await impl.execute(
                "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                (999, "User 999", "user999@example.com", 35),
            )
            await impl.commit()

        times = await self.run_timed(task)
        return BenchmarkResult.from_times("delete_reinsert", impl_name, times)

    # ==================== Workload Benchmarks ====================

    async def bench_concurrent_reads(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> WorkloadResult:
        """Benchmark: Many concurrent read tasks."""
        num_tasks = 100
        task_results: list[TaskResult] = []

        async def read_task(task_id: int) -> TaskResult:
            start = time.perf_counter()
            await impl.execute("SELECT * FROM users WHERE id = ?", (task_id % 1000 + 1,))
            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        overall_start = time.perf_counter()
        task_results = await asyncio.gather(*[read_task(i) for i in range(num_tasks)])
        overall_end = time.perf_counter()

        total_time_ms = (overall_end - overall_start) * 1000
        task_times = [t.duration_ms for t in task_results]

        return WorkloadResult(
            name="concurrent_reads",
            implementation=impl_name,
            total_time_ms=total_time_ms,
            task_results=list(task_results),
            tasks_per_second=num_tasks / (total_time_ms / 1000),
            mean_task_time_ms=statistics.mean(task_times),
            median_task_time_ms=statistics.median(task_times),
        )

    async def bench_concurrent_writes(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> WorkloadResult:
        """Benchmark: Many concurrent write tasks (serialized by writer)."""
        num_tasks = 50
        task_results: list[TaskResult] = []

        async def write_task(task_id: int) -> TaskResult:
            start = time.perf_counter()
            await impl.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                (f"ConcurrentUser{task_id}", f"concurrent{task_id}@test.com", 25),
            )
            await impl.commit()
            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        overall_start = time.perf_counter()
        task_results = await asyncio.gather(*[write_task(i) for i in range(num_tasks)])
        overall_end = time.perf_counter()

        total_time_ms = (overall_end - overall_start) * 1000
        task_times = [t.duration_ms for t in task_results]

        return WorkloadResult(
            name="concurrent_writes",
            implementation=impl_name,
            total_time_ms=total_time_ms,
            task_results=list(task_results),
            tasks_per_second=num_tasks / (total_time_ms / 1000),
            mean_task_time_ms=statistics.mean(task_times),
            median_task_time_ms=statistics.median(task_times),
        )

    async def bench_mixed_workload(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> WorkloadResult:
        """Benchmark: Mixed read and write workload."""
        num_reads = 80
        num_writes = 20
        task_results: list[TaskResult] = []

        async def read_task(task_id: int) -> TaskResult:
            start = time.perf_counter()
            await impl.execute("SELECT * FROM users WHERE age > ?", (25 + task_id % 20,))
            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        async def write_task(task_id: int) -> TaskResult:
            start = time.perf_counter()
            await impl.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                (f"MixedUser{task_id}", f"mixed{task_id}@test.com", 30),
            )
            await impl.commit()
            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        tasks = []
        for i in range(num_reads):
            tasks.append(read_task(i))
        for i in range(num_writes):
            tasks.append(write_task(i + num_reads))

        overall_start = time.perf_counter()
        task_results = await asyncio.gather(*tasks)
        overall_end = time.perf_counter()

        total_time_ms = (overall_end - overall_start) * 1000
        task_times = [t.duration_ms for t in task_results]

        return WorkloadResult(
            name="mixed_workload",
            implementation=impl_name,
            total_time_ms=total_time_ms,
            task_results=list(task_results),
            tasks_per_second=(num_reads + num_writes) / (total_time_ms / 1000),
            mean_task_time_ms=statistics.mean(task_times),
            median_task_time_ms=statistics.median(task_times),
        )

    async def bench_heavy_read_workload(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> WorkloadResult:
        """Benchmark: Heavy concurrent reads with joins."""
        num_tasks = 50
        task_results: list[TaskResult] = []

        async def heavy_read_task(task_id: int) -> TaskResult:
            start = time.perf_counter()
            await impl.execute("""
                SELECT o.id, u.name, p.name, o.quantity
                FROM orders o
                JOIN users u ON o.user_id = u.id
                JOIN products p ON o.product_id = p.id
                WHERE u.age > ?
                LIMIT 50
            """, (20 + task_id % 30,))
            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        overall_start = time.perf_counter()
        task_results = await asyncio.gather(*[heavy_read_task(i) for i in range(num_tasks)])
        overall_end = time.perf_counter()

        total_time_ms = (overall_end - overall_start) * 1000
        task_times = [t.duration_ms for t in task_results]

        return WorkloadResult(
            name="heavy_read_workload",
            implementation=impl_name,
            total_time_ms=total_time_ms,
            task_results=list(task_results),
            tasks_per_second=num_tasks / (total_time_ms / 1000),
            mean_task_time_ms=statistics.mean(task_times),
            median_task_time_ms=statistics.median(task_times),
        )

    async def bench_db_with_io_simulation(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> WorkloadResult:
        """
        Benchmark: DB operations interleaved with simulated async I/O.

        This shows the real benefit of async: doing other work while
        waiting for DB. Simulates 10ms of network I/O between DB calls.
        """
        num_tasks = 20
        task_results: list[TaskResult] = []

        async def db_with_io_task(task_id: int) -> TaskResult:
            start = time.perf_counter()

            # Simulated network call (e.g., fetch user auth from cache)
            await asyncio.sleep(0.01)  # 10ms

            # DB read
            await impl.execute("SELECT * FROM users WHERE id = ?", (task_id % 1000 + 1,))

            # Simulated processing / validation
            await asyncio.sleep(0.005)  # 5ms

            # DB write
            await impl.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                (f"IOUser{task_id}", f"io{task_id}@test.com", 30),
            )
            await impl.commit()

            # Simulated response formatting
            await asyncio.sleep(0.005)  # 5ms

            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        overall_start = time.perf_counter()
        task_results = await asyncio.gather(*[db_with_io_task(i) for i in range(num_tasks)])
        overall_end = time.perf_counter()

        total_time_ms = (overall_end - overall_start) * 1000
        task_times = [t.duration_ms for t in task_results]

        return WorkloadResult(
            name="db_with_io_simulation",
            implementation=impl_name,
            total_time_ms=total_time_ms,
            task_results=list(task_results),
            tasks_per_second=num_tasks / (total_time_ms / 1000),
            mean_task_time_ms=statistics.mean(task_times),
            median_task_time_ms=statistics.median(task_times),
        )

    # ==================== Transaction Benchmarks ====================

    async def bench_transaction_simple(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Simple transaction with a few operations."""
        counter = [0]

        async def task() -> None:
            counter[0] += 1
            base_id = 1000000 + counter[0] * 10

            async def ops(db: BaseSQLiteImplementation) -> None:
                await db.execute(
                    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                    (base_id, f"TxUser{base_id}", f"tx{base_id}@test.com", 25),
                )
                await db.execute(
                    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                    (base_id + 1, f"TxUser{base_id + 1}", f"tx{base_id + 1}@test.com", 26),
                )
                await db.execute("SELECT * FROM users WHERE id = ?", (base_id,))

            await impl.run_in_transaction(ops)

        times = await self.run_timed(task, iterations=100)
        return BenchmarkResult.from_times("transaction_simple", impl_name, times)

    async def bench_transaction_batch(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Transaction with batch insert."""
        counter = [0]

        async def task() -> None:
            counter[0] += 1
            base_id = 2000000 + counter[0] * 100

            async def ops(db: BaseSQLiteImplementation) -> None:
                rows = [
                    (base_id + i, f"BatchTx{base_id + i}", f"batchtx{base_id + i}@test.com", 30)
                    for i in range(50)
                ]
                await db.executemany(
                    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                    rows,
                )
                # Read to verify
                await db.execute("SELECT COUNT(*) FROM users WHERE id >= ?", (base_id,))

            await impl.run_in_transaction(ops)

        times = await self.run_timed(task, iterations=50)
        return BenchmarkResult.from_times("transaction_batch", impl_name, times)

    async def bench_concurrent_transactions(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> WorkloadResult:
        """Benchmark: Many concurrent transactions (serialized)."""
        num_tasks = 20
        task_results: list[TaskResult] = []

        async def tx_task(task_id: int) -> TaskResult:
            start = time.perf_counter()
            base_id = 3000000 + task_id * 10

            async def ops(db: BaseSQLiteImplementation) -> None:
                await db.execute(
                    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                    (base_id, f"ConcTx{base_id}", f"conctx{base_id}@test.com", 25),
                )
                await db.execute(
                    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                    (base_id + 1, f"ConcTx{base_id + 1}", f"conctx{base_id + 1}@test.com", 26),
                )
                await db.execute("SELECT * FROM users WHERE id >= ?", (base_id,))
                await db.execute(
                    "UPDATE users SET age = age + 1 WHERE id = ?", (base_id,)
                )

            await impl.run_in_transaction(ops)
            end = time.perf_counter()
            return TaskResult(
                task_id=task_id,
                start_time=start,
                end_time=end,
                duration_ms=(end - start) * 1000,
            )

        overall_start = time.perf_counter()
        task_results = await asyncio.gather(*[tx_task(i) for i in range(num_tasks)])
        overall_end = time.perf_counter()

        total_time_ms = (overall_end - overall_start) * 1000
        task_times = [t.duration_ms for t in task_results]

        return WorkloadResult(
            name="concurrent_transactions",
            implementation=impl_name,
            total_time_ms=total_time_ms,
            task_results=list(task_results),
            tasks_per_second=num_tasks / (total_time_ms / 1000),
            mean_task_time_ms=statistics.mean(task_times),
            median_task_time_ms=statistics.median(task_times),
        )

    async def bench_transaction_with_reads(
        self, impl: BaseSQLiteImplementation, impl_name: str
    ) -> BenchmarkResult:
        """Benchmark: Transaction with multiple reads and writes."""
        counter = [0]

        async def task() -> None:
            counter[0] += 1
            base_id = 4000000 + counter[0] * 10

            async def ops(db: BaseSQLiteImplementation) -> None:
                # Read existing data
                await db.execute("SELECT * FROM users WHERE age > ? LIMIT 10", (30,))

                # Insert
                await db.execute(
                    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
                    (base_id, f"TxRead{base_id}", f"txread{base_id}@test.com", 35),
                )

                # Read our insert
                await db.execute("SELECT * FROM users WHERE id = ?", (base_id,))

                # Update based on read
                await db.execute(
                    "UPDATE users SET age = ? WHERE id = ?", (36, base_id)
                )

                # Verify update
                await db.execute("SELECT age FROM users WHERE id = ?", (base_id,))

            await impl.run_in_transaction(ops)

        times = await self.run_timed(task, iterations=100)
        return BenchmarkResult.from_times("transaction_with_reads", impl_name, times)


def print_benchmark_results(results: list[BenchmarkResult]) -> None:
    """Print benchmark results in a formatted table."""
    if not results:
        return

    name = results[0].name
    print(f"\n{'=' * 80}")
    print(f"Benchmark: {name} ({results[0].iterations} iterations)")
    print(f"{'=' * 80}")
    print(
        f"{'Implementation':<20} {'Mean (ms)':<12} {'Median (ms)':<12} "
        f"{'Min (ms)':<12} {'Max (ms)':<12} {'Std Dev':<12}"
    )
    print("-" * 80)

    for r in sorted(results, key=lambda x: x.mean_time_ms):
        print(
            f"{r.implementation:<20} {r.mean_time_ms:<12.3f} {r.median_time_ms:<12.3f} "
            f"{r.min_time_ms:<12.3f} {r.max_time_ms:<12.3f} {r.std_dev_ms:<12.3f}"
        )


def print_workload_results(results: list[WorkloadResult]) -> None:
    """Print workload results in a formatted table."""
    if not results:
        return

    name = results[0].name
    print(f"\n{'=' * 80}")
    print(f"Workload: {name}")
    print(f"{'=' * 80}")
    print(
        f"{'Implementation':<20} {'Total (ms)':<12} {'Tasks/sec':<12} "
        f"{'Mean Task (ms)':<15} {'Median Task (ms)':<15}"
    )
    print("-" * 80)

    for r in sorted(results, key=lambda x: x.total_time_ms):
        print(
            f"{r.implementation:<20} {r.total_time_ms:<12.2f} {r.tasks_per_second:<12.1f} "
            f"{r.mean_task_time_ms:<15.3f} {r.median_task_time_ms:<15.3f}"
        )


def print_summary(all_results: dict[str, list[BenchmarkResult | WorkloadResult]]) -> None:
    """Print overall summary comparing implementations."""
    print("\n" + "=" * 80)
    print("SUMMARY: Speed comparison (relative to main_thread baseline)")
    print("=" * 80)

    for bench_name, results in all_results.items():
        if isinstance(results[0], BenchmarkResult):
            baseline = next(
                (r for r in results if r.implementation == "main_thread"), None
            )
            if baseline:
                print(f"\n{bench_name}:")
                for r in results:
                    if isinstance(r, BenchmarkResult):
                        speedup = baseline.mean_time_ms / r.mean_time_ms
                        faster = "faster" if speedup > 1 else "slower"
                        print(f"  {r.implementation:<20}: {speedup:.2f}x {faster}")
        else:
            baseline = next(
                (r for r in results if r.implementation == "main_thread"), None
            )
            if baseline:
                print(f"\n{bench_name}:")
                for r in results:
                    if isinstance(r, WorkloadResult):
                        speedup = baseline.total_time_ms / r.total_time_ms
                        faster = "faster" if speedup > 1 else "slower"
                        throughput_ratio = r.tasks_per_second / baseline.tasks_per_second
                        print(
                            f"  {r.implementation:<20}: {speedup:.2f}x {faster} "
                            f"({throughput_ratio:.2f}x throughput)"
                        )


async def run_all_benchmarks() -> None:
    """Run all benchmarks for all implementations."""
    implementations = [
        ("main_thread", MainThreadSQLite()),
        ("single_thread", SingleThreadSQLite()),
        ("pasyn_pool", PasynPoolSQLite(num_readers=1)),
    ]

    all_results: dict[str, list[BenchmarkResult | WorkloadResult]] = {}

    for impl_name, impl in implementations:
        print(f"\n{'#' * 80}")
        print(f"# Running benchmarks for: {impl_name}")
        print(f"{'#' * 80}")

        # Create fresh database for each implementation
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            await impl.setup(db_path)
            bench = Benchmark(db_path)

            # Setup and populate
            print("Setting up schema and test data...")
            await bench.setup_schema(impl)
            await bench.populate_test_data(impl)
            print("Done.")

            # Run statement benchmarks
            statement_benchmarks = [
                ("small_read", bench.bench_small_read),
                ("large_read", bench.bench_large_read),
                ("large_read_ordered", bench.bench_large_read_ordered),
                ("join_small", bench.bench_join_small),
                ("join_large", bench.bench_join_large),
                ("aggregate", bench.bench_aggregate),
                ("small_write", bench.bench_small_write),
                ("large_write", bench.bench_large_write),
                ("batch_write", bench.bench_batch_write),
                ("update", bench.bench_update),
                ("delete_reinsert", bench.bench_delete),
            ]

            for name, benchmark_func in statement_benchmarks:
                print(f"  Running {name}...", end=" ", flush=True)
                result = await benchmark_func(impl, impl_name)
                print(f"done ({result.mean_time_ms:.3f}ms mean)")

                if name not in all_results:
                    all_results[name] = []
                all_results[name].append(result)

            # Run workload benchmarks
            workload_benchmarks = [
                ("concurrent_reads", bench.bench_concurrent_reads),
                ("concurrent_writes", bench.bench_concurrent_writes),
                ("mixed_workload", bench.bench_mixed_workload),
                ("heavy_read_workload", bench.bench_heavy_read_workload),
                ("db_with_io_simulation", bench.bench_db_with_io_simulation),
            ]

            for name, benchmark_func in workload_benchmarks:
                print(f"  Running {name}...", end=" ", flush=True)
                result = await benchmark_func(impl, impl_name)
                print(f"done ({result.total_time_ms:.2f}ms total)")

                if name not in all_results:
                    all_results[name] = []
                all_results[name].append(result)

            # Run transaction benchmarks
            transaction_benchmarks = [
                ("transaction_simple", bench.bench_transaction_simple),
                ("transaction_batch", bench.bench_transaction_batch),
                ("transaction_with_reads", bench.bench_transaction_with_reads),
            ]

            for name, benchmark_func in transaction_benchmarks:
                print(f"  Running {name}...", end=" ", flush=True)
                result = await benchmark_func(impl, impl_name)
                print(f"done ({result.mean_time_ms:.3f}ms mean)")

                if name not in all_results:
                    all_results[name] = []
                all_results[name].append(result)

            # Run concurrent transactions workload
            transaction_workloads = [
                ("concurrent_transactions", bench.bench_concurrent_transactions),
            ]

            for name, benchmark_func in transaction_workloads:
                print(f"  Running {name}...", end=" ", flush=True)
                result = await benchmark_func(impl, impl_name)
                print(f"done ({result.total_time_ms:.2f}ms total)")

                if name not in all_results:
                    all_results[name] = []
                all_results[name].append(result)

            await impl.close()

        finally:
            Path(db_path).unlink(missing_ok=True)
            # Clean up WAL files
            Path(f"{db_path}-wal").unlink(missing_ok=True)
            Path(f"{db_path}-shm").unlink(missing_ok=True)

    # Print detailed results
    print("\n\n" + "#" * 80)
    print("# DETAILED RESULTS")
    print("#" * 80)

    # Statement benchmarks
    statement_names = [
        "small_read", "large_read", "large_read_ordered",
        "join_small", "join_large", "aggregate",
        "small_write", "large_write", "batch_write",
        "update", "delete_reinsert",
    ]
    for name in statement_names:
        if name in all_results:
            print_benchmark_results(all_results[name])  # type: ignore

    # Transaction benchmarks
    transaction_names = [
        "transaction_simple", "transaction_batch", "transaction_with_reads",
    ]
    for name in transaction_names:
        if name in all_results:
            print_benchmark_results(all_results[name])  # type: ignore

    # Workload benchmarks
    workload_names = [
        "concurrent_reads", "concurrent_writes",
        "mixed_workload", "heavy_read_workload",
        "db_with_io_simulation", "concurrent_transactions",
    ]
    for name in workload_names:
        if name in all_results:
            print_workload_results(all_results[name])  # type: ignore

    # Print summary
    print_summary(all_results)


if __name__ == "__main__":
    asyncio.run(run_all_benchmarks())
