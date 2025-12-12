#!/usr/bin/env python3
"""
Multiprocess benchmark for pasyn-sqlite.

Tests multiple processes reading and writing to the same database concurrently.
Each process runs an async event loop with concurrent operations.
Compares all three implementations: main_thread, single_thread, multiplexed.
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import queue
import random
import sqlite3
import statistics
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from multiprocessing import Queue
from pathlib import Path
from typing import Any, Callable

import pasyn_sqlite_core


@dataclass
class ProcessResult:
    """Results from a single process."""

    process_id: int
    total_reads: int
    total_writes: int
    read_latencies_ms: list[float] = field(default_factory=list)
    write_latencies_ms: list[float] = field(default_factory=list)
    elapsed_time_s: float = 0.0
    errors: list[str] = field(default_factory=list)


@dataclass
class BenchmarkConfig:
    """Configuration for the benchmark."""

    num_processes: int = 4
    operations_per_process: int = 100
    concurrent_tasks_per_process: int = 10
    read_write_ratio: float = 0.8  # 80% reads, 20% writes
    warmup_operations: int = 10
    db_path: str = ""
    socket_path: str = ""
    implementation: str = "multiplexed"


@dataclass
class ImplementationResult:
    """Results for a single implementation."""

    name: str
    total_elapsed_s: float
    total_operations: int
    total_reads: int
    total_writes: int
    throughput_ops_per_s: float
    read_throughput: float
    write_throughput: float
    read_latency_mean_ms: float
    read_latency_median_ms: float
    read_latency_p95_ms: float
    write_latency_mean_ms: float
    write_latency_median_ms: float
    write_latency_p95_ms: float
    errors: int


def setup_database(db_path: str) -> None:
    """Create and populate the test database."""
    conn = pasyn_sqlite_core.connect(db_path)

    # Create tables
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Create indices for faster reads
    conn.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id)")

    # Insert initial data
    existing = conn.execute_fetchall("SELECT COUNT(*) FROM users")
    if existing[0][0] == 0:
        for i in range(1000):
            conn.execute(
                "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                [f"User {i}", f"user{i}@example.com", 20 + (i % 60)],
            )

    conn.close()


# =============================================================================
# MAIN THREAD IMPLEMENTATION (each process has its own connection)
# =============================================================================


async def main_thread_worker_task(
    task_id: int,
    config: BenchmarkConfig,
    conn: sqlite3.Connection,
    results_collector: dict[str, list[float]],
) -> tuple[int, int, list[str]]:
    """Worker task for main_thread implementation."""
    reads = 0
    writes = 0
    errors: list[str] = []

    ops_per_task = config.operations_per_process // config.concurrent_tasks_per_process

    for _ in range(ops_per_task):
        is_read = random.random() < config.read_write_ratio

        try:
            if is_read:
                start = time.perf_counter()
                query_type = random.randint(0, 2)
                if query_type == 0:
                    user_id = random.randint(1, 1000)
                    conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchall()
                elif query_type == 1:
                    min_age = random.randint(20, 50)
                    conn.execute(
                        "SELECT * FROM users WHERE age >= ? AND age < ? LIMIT 50",
                        (min_age, min_age + 10),
                    ).fetchall()
                else:
                    conn.execute(
                        "SELECT COUNT(*) FROM events WHERE user_id = ?",
                        (random.randint(1, 1000),),
                    ).fetchall()

                elapsed = (time.perf_counter() - start) * 1000
                results_collector["read_latencies"].append(elapsed)
                reads += 1
            else:
                start = time.perf_counter()
                user_id = random.randint(1, 1000)
                event_type = random.choice(["click", "view", "purchase", "signup"])
                payload = f'{{"task": {task_id}, "data": "{random.random()}"}}'
                conn.execute(
                    "INSERT INTO events (user_id, event_type, payload) VALUES (?, ?, ?)",
                    (user_id, event_type, payload),
                )
                elapsed = (time.perf_counter() - start) * 1000
                results_collector["write_latencies"].append(elapsed)
                writes += 1

        except Exception as e:
            errors.append(f"Task {task_id}: {type(e).__name__}: {e}")

        await asyncio.sleep(0)

    return reads, writes, errors


async def run_main_thread_worker(config: BenchmarkConfig) -> ProcessResult:
    """Run main_thread implementation worker."""
    process_id = os.getpid()
    result = ProcessResult(process_id=process_id, total_reads=0, total_writes=0)

    # Each process has its own connection with WAL mode
    conn = sqlite3.connect(config.db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    results_collector: dict[str, list[float]] = {
        "read_latencies": [],
        "write_latencies": [],
    }

    # Warmup
    for _ in range(config.warmup_operations):
        conn.execute("SELECT * FROM users WHERE id = ?", (1,)).fetchall()

    start_time = time.perf_counter()

    tasks = [
        main_thread_worker_task(i, config, conn, results_collector)
        for i in range(config.concurrent_tasks_per_process)
    ]

    task_results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - start_time

    for task_result in task_results:
        if isinstance(task_result, Exception):
            result.errors.append(f"Task exception: {task_result}")
        else:
            reads, writes, errors = task_result
            result.total_reads += reads
            result.total_writes += writes
            result.errors.extend(errors)

    result.read_latencies_ms = results_collector["read_latencies"]
    result.write_latencies_ms = results_collector["write_latencies"]
    result.elapsed_time_s = elapsed

    conn.close()
    return result


# =============================================================================
# SINGLE THREAD IMPLEMENTATION (each process has worker thread + queue)
# =============================================================================


@dataclass
class DBTask:
    """Task for the single-thread DB worker."""

    func: Callable[..., Any]
    future: asyncio.Future[Any]


class SingleThreadDB:
    """Single-thread database handler for a process."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._queue: queue.Queue[DBTask | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._conn: sqlite3.Connection | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._thread.start()

    def _worker_loop(self) -> None:
        self._conn = sqlite3.connect(self._db_path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

        while True:
            task = self._queue.get()
            if task is None:
                break
            try:
                result = task.func(self._conn)
                if self._loop:
                    self._loop.call_soon_threadsafe(task.future.set_result, result)
            except Exception as e:
                if self._loop:
                    self._loop.call_soon_threadsafe(task.future.set_exception, e)

        if self._conn:
            self._conn.close()

    async def execute(self, func: Callable[[sqlite3.Connection], Any]) -> Any:
        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        task = DBTask(func=func, future=future)
        self._queue.put(task)
        return await future

    def stop(self) -> None:
        self._queue.put(None)
        if self._thread:
            self._thread.join(timeout=5.0)


async def single_thread_worker_task(
    task_id: int,
    config: BenchmarkConfig,
    db: SingleThreadDB,
    results_collector: dict[str, list[float]],
) -> tuple[int, int, list[str]]:
    """Worker task for single_thread implementation."""
    reads = 0
    writes = 0
    errors: list[str] = []

    ops_per_task = config.operations_per_process // config.concurrent_tasks_per_process

    for _ in range(ops_per_task):
        is_read = random.random() < config.read_write_ratio

        try:
            if is_read:
                start = time.perf_counter()
                query_type = random.randint(0, 2)
                if query_type == 0:
                    user_id = random.randint(1, 1000)
                    await db.execute(
                        lambda conn, uid=user_id: conn.execute(
                            "SELECT * FROM users WHERE id = ?", (uid,)
                        ).fetchall()
                    )
                elif query_type == 1:
                    min_age = random.randint(20, 50)
                    await db.execute(
                        lambda conn, ma=min_age: conn.execute(
                            "SELECT * FROM users WHERE age >= ? AND age < ? LIMIT 50",
                            (ma, ma + 10),
                        ).fetchall()
                    )
                else:
                    uid = random.randint(1, 1000)
                    await db.execute(
                        lambda conn, u=uid: conn.execute(
                            "SELECT COUNT(*) FROM events WHERE user_id = ?", (u,)
                        ).fetchall()
                    )

                elapsed = (time.perf_counter() - start) * 1000
                results_collector["read_latencies"].append(elapsed)
                reads += 1
            else:
                start = time.perf_counter()
                user_id = random.randint(1, 1000)
                event_type = random.choice(["click", "view", "purchase", "signup"])
                payload = f'{{"task": {task_id}, "data": "{random.random()}"}}'

                await db.execute(
                    lambda conn, uid=user_id, et=event_type, pl=payload: conn.execute(
                        "INSERT INTO events (user_id, event_type, payload) VALUES (?, ?, ?)",
                        (uid, et, pl),
                    )
                )

                elapsed = (time.perf_counter() - start) * 1000
                results_collector["write_latencies"].append(elapsed)
                writes += 1

        except Exception as e:
            errors.append(f"Task {task_id}: {type(e).__name__}: {e}")

        await asyncio.sleep(0)

    return reads, writes, errors


async def run_single_thread_worker(config: BenchmarkConfig) -> ProcessResult:
    """Run single_thread implementation worker."""
    process_id = os.getpid()
    result = ProcessResult(process_id=process_id, total_reads=0, total_writes=0)

    loop = asyncio.get_event_loop()
    db = SingleThreadDB(config.db_path)
    db.start(loop)

    results_collector: dict[str, list[float]] = {
        "read_latencies": [],
        "write_latencies": [],
    }

    # Warmup
    for _ in range(config.warmup_operations):
        await db.execute(
            lambda conn: conn.execute("SELECT * FROM users WHERE id = ?", (1,)).fetchall()
        )

    start_time = time.perf_counter()

    tasks = [
        single_thread_worker_task(i, config, db, results_collector)
        for i in range(config.concurrent_tasks_per_process)
    ]

    task_results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - start_time

    for task_result in task_results:
        if isinstance(task_result, Exception):
            result.errors.append(f"Task exception: {task_result}")
        else:
            reads, writes, errors = task_result
            result.total_reads += reads
            result.total_writes += writes
            result.errors.extend(errors)

    result.read_latencies_ms = results_collector["read_latencies"]
    result.write_latencies_ms = results_collector["write_latencies"]
    result.elapsed_time_s = elapsed

    db.stop()
    return result


# =============================================================================
# MULTIPLEXED IMPLEMENTATION (shared server, per-process client)
# =============================================================================


async def multiplexed_worker_task(
    task_id: int,
    config: BenchmarkConfig,
    client: pasyn_sqlite_core.MultiplexedClient,
    read_conn: pasyn_sqlite_core.Connection,
    results_collector: dict[str, list[float]],
) -> tuple[int, int, list[str]]:
    """Worker task for multiplexed implementation."""
    reads = 0
    writes = 0
    errors: list[str] = []

    ops_per_task = config.operations_per_process // config.concurrent_tasks_per_process

    for _ in range(ops_per_task):
        is_read = random.random() < config.read_write_ratio

        try:
            if is_read:
                start = time.perf_counter()
                query_type = random.randint(0, 2)
                if query_type == 0:
                    user_id = random.randint(1, 1000)
                    read_conn.execute_fetchall("SELECT * FROM users WHERE id = ?", [user_id])
                elif query_type == 1:
                    min_age = random.randint(20, 50)
                    read_conn.execute_fetchall(
                        "SELECT * FROM users WHERE age >= ? AND age < ? LIMIT 50",
                        [min_age, min_age + 10],
                    )
                else:
                    read_conn.execute_fetchall(
                        "SELECT COUNT(*) FROM events WHERE user_id = ?",
                        [random.randint(1, 1000)],
                    )

                elapsed = (time.perf_counter() - start) * 1000
                results_collector["read_latencies"].append(elapsed)
                reads += 1
            else:
                start = time.perf_counter()
                user_id = random.randint(1, 1000)
                event_type = random.choice(["click", "view", "purchase", "signup"])
                payload = f'{{"task": {task_id}, "data": "{random.random()}"}}'
                client.execute(
                    "INSERT INTO events (user_id, event_type, payload) VALUES (?, ?, ?)",
                    [user_id, event_type, payload],
                )

                elapsed = (time.perf_counter() - start) * 1000
                results_collector["write_latencies"].append(elapsed)
                writes += 1

        except Exception as e:
            errors.append(f"Task {task_id}: {type(e).__name__}: {e}")

        await asyncio.sleep(0)

    return reads, writes, errors


async def run_multiplexed_worker(config: BenchmarkConfig) -> ProcessResult:
    """Run multiplexed implementation worker."""
    process_id = os.getpid()
    result = ProcessResult(process_id=process_id, total_reads=0, total_writes=0)

    client = pasyn_sqlite_core.MultiplexedClient(config.socket_path)
    read_conn = pasyn_sqlite_core.connect(config.db_path, pasyn_sqlite_core.OpenFlags.readonly())

    results_collector: dict[str, list[float]] = {
        "read_latencies": [],
        "write_latencies": [],
    }

    # Warmup
    for _ in range(config.warmup_operations):
        read_conn.execute_fetchall("SELECT * FROM users WHERE id = ?", [1])

    start_time = time.perf_counter()

    tasks = [
        multiplexed_worker_task(i, config, client, read_conn, results_collector)
        for i in range(config.concurrent_tasks_per_process)
    ]

    task_results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - start_time

    for task_result in task_results:
        if isinstance(task_result, Exception):
            result.errors.append(f"Task exception: {task_result}")
        else:
            reads, writes, errors = task_result
            result.total_reads += reads
            result.total_writes += writes
            result.errors.extend(errors)

    result.read_latencies_ms = results_collector["read_latencies"]
    result.write_latencies_ms = results_collector["write_latencies"]
    result.elapsed_time_s = elapsed

    read_conn.close()
    return result


# =============================================================================
# WORKER PROCESS ENTRY POINTS
# =============================================================================


def worker_process(config: BenchmarkConfig, result_queue: Queue[ProcessResult]) -> None:
    """Entry point for worker processes."""
    try:
        if config.implementation == "main_thread":
            result = asyncio.run(run_main_thread_worker(config))
        elif config.implementation == "single_thread":
            result = asyncio.run(run_single_thread_worker(config))
        elif config.implementation == "multiplexed":
            result = asyncio.run(run_multiplexed_worker(config))
        else:
            raise ValueError(f"Unknown implementation: {config.implementation}")
        result_queue.put(result)
    except Exception as e:
        result = ProcessResult(
            process_id=os.getpid(),
            total_reads=0,
            total_writes=0,
            errors=[f"Process error: {type(e).__name__}: {e}"],
        )
        result_queue.put(result)


# =============================================================================
# BENCHMARK RUNNER
# =============================================================================


def format_stats(values: list[float], unit: str = "ms") -> str:
    """Format statistics for a list of values."""
    if not values:
        return "N/A"

    p95_idx = min(int(len(values) * 0.95), len(values) - 1)
    return (
        f"min={min(values):.2f}{unit}, "
        f"max={max(values):.2f}{unit}, "
        f"mean={statistics.mean(values):.2f}{unit}, "
        f"median={statistics.median(values):.2f}{unit}, "
        f"p95={sorted(values)[p95_idx]:.2f}{unit}"
    )


def get_percentile(values: list[float], percentile: float) -> float:
    """Get percentile value from a list."""
    if not values:
        return 0.0
    idx = min(int(len(values) * percentile), len(values) - 1)
    return sorted(values)[idx]


def run_single_implementation(
    config: BenchmarkConfig, server: Any | None = None
) -> ImplementationResult:
    """Run benchmark for a single implementation."""
    impl_name = config.implementation

    # For multiplexed, we need the server
    if impl_name == "multiplexed" and server:
        config.socket_path = server.socket_path

    result_queue: Queue[ProcessResult] = mp.Queue()

    start_time = time.perf_counter()

    processes: list[mp.Process] = []
    for _ in range(config.num_processes):
        p = mp.Process(target=worker_process, args=(config, result_queue))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    total_elapsed = time.perf_counter() - start_time

    # Collect results
    results: list[ProcessResult] = []
    while not result_queue.empty():
        results.append(result_queue.get())

    total_reads = 0
    total_writes = 0
    all_read_latencies: list[float] = []
    all_write_latencies: list[float] = []
    all_errors: list[str] = []

    for result in results:
        total_reads += result.total_reads
        total_writes += result.total_writes
        all_read_latencies.extend(result.read_latencies_ms)
        all_write_latencies.extend(result.write_latencies_ms)
        all_errors.extend(result.errors)

    return ImplementationResult(
        name=impl_name,
        total_elapsed_s=total_elapsed,
        total_operations=total_reads + total_writes,
        total_reads=total_reads,
        total_writes=total_writes,
        throughput_ops_per_s=(total_reads + total_writes) / total_elapsed if total_elapsed > 0 else 0,
        read_throughput=total_reads / total_elapsed if total_elapsed > 0 else 0,
        write_throughput=total_writes / total_elapsed if total_elapsed > 0 else 0,
        read_latency_mean_ms=statistics.mean(all_read_latencies) if all_read_latencies else 0,
        read_latency_median_ms=statistics.median(all_read_latencies) if all_read_latencies else 0,
        read_latency_p95_ms=get_percentile(all_read_latencies, 0.95),
        write_latency_mean_ms=statistics.mean(all_write_latencies) if all_write_latencies else 0,
        write_latency_median_ms=statistics.median(all_write_latencies) if all_write_latencies else 0,
        write_latency_p95_ms=get_percentile(all_write_latencies, 0.95),
        errors=len(all_errors),
    )


def run_all_benchmarks(config: BenchmarkConfig) -> dict[str, ImplementationResult]:
    """Run benchmarks for all implementations."""
    implementations = ["main_thread", "multiplexed"]
    results: dict[str, ImplementationResult] = {}

    print(f"\n{'#' * 80}")
    print("# MULTIPROCESS BENCHMARK - ALL IMPLEMENTATIONS")
    print(f"{'#' * 80}")
    print(f"\nConfiguration:")
    print(f"  Processes: {config.num_processes}")
    print(f"  Operations per process: {config.operations_per_process}")
    print(f"  Concurrent tasks per process: {config.concurrent_tasks_per_process}")
    print(f"  Read/Write ratio: {config.read_write_ratio:.0%} / {1 - config.read_write_ratio:.0%}")
    print(f"  Total expected operations: {config.num_processes * config.operations_per_process}")

    # Setup database once
    print(f"\nSetting up database at: {config.db_path}")
    setup_database(config.db_path)

    # Start multiplexed server (needed for multiplexed implementation)
    print("Starting writer server for multiplexed implementation...")
    server = pasyn_sqlite_core.start_writer_server(config.db_path)
    print(f"Writer server started at: {server.socket_path}")

    for impl_name in implementations:
        print(f"\n{'=' * 60}")
        print(f"Running: {impl_name}")
        print(f"{'=' * 60}")

        config.implementation = impl_name
        result = run_single_implementation(config, server if impl_name == "multiplexed" else None)
        results[impl_name] = result

        print(f"  Completed in {result.total_elapsed_s:.2f}s")
        print(f"  Throughput: {result.throughput_ops_per_s:.1f} ops/s")
        if result.errors > 0:
            print(f"  Errors: {result.errors}")

    # Stop server
    server.stop()

    return results


def print_comparison_table(results: dict[str, ImplementationResult]) -> None:
    """Print comparison table of all implementations."""
    print(f"\n{'#' * 80}")
    print("# RESULTS COMPARISON")
    print(f"{'#' * 80}\n")

    # Header
    print(f"{'Metric':<30} {'main_thread':>15} {'multiplexed':>15}")
    print("-" * 65)

    # Get results
    mt = results.get("main_thread")
    mx = results.get("multiplexed")

    if not all([mt, mx]):
        print("Missing results for some implementations")
        return

    # Throughput
    print(f"{'Total Time (s)':<30} {mt.total_elapsed_s:>15.2f} {mx.total_elapsed_s:>15.2f}")
    print(f"{'Total Operations':<30} {mt.total_operations:>15} {mx.total_operations:>15}")
    print(f"{'Throughput (ops/s)':<30} {mt.throughput_ops_per_s:>15.1f} {mx.throughput_ops_per_s:>15.1f}")
    print(f"{'Read Throughput (ops/s)':<30} {mt.read_throughput:>15.1f} {mx.read_throughput:>15.1f}")
    print(f"{'Write Throughput (ops/s)':<30} {mt.write_throughput:>15.1f} {mx.write_throughput:>15.1f}")

    print()
    print(f"{'Read Latency Mean (ms)':<30} {mt.read_latency_mean_ms:>15.3f} {mx.read_latency_mean_ms:>15.3f}")
    print(f"{'Read Latency Median (ms)':<30} {mt.read_latency_median_ms:>15.3f} {mx.read_latency_median_ms:>15.3f}")
    print(f"{'Read Latency P95 (ms)':<30} {mt.read_latency_p95_ms:>15.3f} {mx.read_latency_p95_ms:>15.3f}")

    print()
    print(f"{'Write Latency Mean (ms)':<30} {mt.write_latency_mean_ms:>15.3f} {mx.write_latency_mean_ms:>15.3f}")
    print(f"{'Write Latency Median (ms)':<30} {mt.write_latency_median_ms:>15.3f} {mx.write_latency_median_ms:>15.3f}")
    print(f"{'Write Latency P95 (ms)':<30} {mt.write_latency_p95_ms:>15.3f} {mx.write_latency_p95_ms:>15.3f}")

    print()
    print(f"{'Errors':<30} {mt.errors:>15} {mx.errors:>15}")

    # Relative performance
    print(f"\n{'=' * 80}")
    print("RELATIVE PERFORMANCE (vs main_thread baseline)")
    print(f"{'=' * 80}\n")

    baseline_throughput = mt.throughput_ops_per_s
    if baseline_throughput > 0:
        print(f"{'Implementation':<20} {'Throughput Ratio':>20} {'Assessment':>20}")
        print("-" * 60)
        for name, result in results.items():
            ratio = result.throughput_ops_per_s / baseline_throughput
            if ratio > 1.05:
                assessment = f"{ratio:.2f}x faster"
            elif ratio < 0.95:
                assessment = f"{ratio:.2f}x slower"
            else:
                assessment = "~same"
            print(f"{name:<20} {ratio:>20.2f}x {assessment:>20}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Multiprocess benchmark for pasyn-sqlite (all implementations)"
    )
    parser.add_argument(
        "--processes",
        "-p",
        type=int,
        default=4,
        help="Number of worker processes (default: 4)",
    )
    parser.add_argument(
        "--operations",
        "-o",
        type=int,
        default=1000,
        help="Operations per process (default: 1000)",
    )
    parser.add_argument(
        "--concurrent",
        "-c",
        type=int,
        default=10,
        help="Concurrent tasks per process (default: 10)",
    )
    parser.add_argument(
        "--read-ratio",
        "-r",
        type=float,
        default=0.8,
        help="Read ratio (0.0-1.0, default: 0.8 = 80%% reads)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="",
        help="Database path (default: temp file)",
    )
    args = parser.parse_args()

    # Use temp file if no db path specified
    if args.db_path:
        db_path = args.db_path
        cleanup_db = False
    else:
        fd, db_path = tempfile.mkstemp(suffix=".db", prefix="multiprocess_bench_")
        os.close(fd)
        cleanup_db = True

    try:
        config = BenchmarkConfig(
            num_processes=args.processes,
            operations_per_process=args.operations,
            concurrent_tasks_per_process=args.concurrent,
            read_write_ratio=args.read_ratio,
            db_path=db_path,
        )

        results = run_all_benchmarks(config)
        print_comparison_table(results)

        total_errors = sum(r.errors for r in results.values())
        if total_errors > 0:
            print(f"\nWarning: {total_errors} total errors occurred")
            return 1

        return 0

    finally:
        # Cleanup temp files
        if cleanup_db:
            for suffix in ["", "-wal", "-shm"]:
                path = Path(db_path + suffix)
                if path.exists():
                    path.unlink()


if __name__ == "__main__":
    # Use spawn to ensure clean process state (important for multiprocessing with asyncio)
    mp.set_start_method("spawn", force=True)
    sys.exit(main())
