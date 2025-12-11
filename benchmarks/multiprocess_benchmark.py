#!/usr/bin/env python3
"""
Multiprocess benchmark for pasyn-sqlite.

Tests multiple processes reading and writing to the same database concurrently.
Each process runs an async event loop with concurrent operations.
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import random
import statistics
import sys
import tempfile
import time
from dataclasses import dataclass, field
from multiprocessing import Queue
from pathlib import Path
from typing import Any

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


async def worker_task(
    task_id: int,
    config: BenchmarkConfig,
    client: pasyn_sqlite_core.MultiplexedClient,
    read_conn: pasyn_sqlite_core.Connection,
    results_collector: dict[str, list[float]],
) -> tuple[int, int, list[str]]:
    """
    Single async task that performs reads and writes.

    Returns (read_count, write_count, errors).
    """
    reads = 0
    writes = 0
    errors: list[str] = []

    ops_per_task = config.operations_per_process // config.concurrent_tasks_per_process

    for _ in range(ops_per_task):
        is_read = random.random() < config.read_write_ratio

        try:
            if is_read:
                # Read operation
                start = time.perf_counter()

                # Random read query
                query_type = random.randint(0, 2)
                if query_type == 0:
                    # Simple lookup
                    user_id = random.randint(1, 1000)
                    read_conn.execute_fetchall("SELECT * FROM users WHERE id = ?", [user_id])
                elif query_type == 1:
                    # Range query
                    min_age = random.randint(20, 50)
                    read_conn.execute_fetchall(
                        "SELECT * FROM users WHERE age >= ? AND age < ? LIMIT 50",
                        [min_age, min_age + 10],
                    )
                else:
                    # Count query
                    read_conn.execute_fetchall(
                        "SELECT COUNT(*) FROM events WHERE user_id = ?",
                        [random.randint(1, 1000)],
                    )

                elapsed = (time.perf_counter() - start) * 1000
                results_collector["read_latencies"].append(elapsed)
                reads += 1
            else:
                # Write operation
                start = time.perf_counter()

                # Insert event
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

        # Small yield to allow other tasks to run
        await asyncio.sleep(0)

    return reads, writes, errors


async def run_worker_async(config: BenchmarkConfig) -> ProcessResult:
    """Run the async workload for a single process."""
    process_id = os.getpid()
    result = ProcessResult(process_id=process_id, total_reads=0, total_writes=0)

    # Connect to the shared writer server
    client = pasyn_sqlite_core.MultiplexedClient(config.socket_path)

    # Create local read-only connection
    read_conn = pasyn_sqlite_core.connect(config.db_path, pasyn_sqlite_core.OpenFlags.readonly())

    # Shared results collector (single-process, no locking needed)
    results_collector: dict[str, list[float]] = {
        "read_latencies": [],
        "write_latencies": [],
    }

    # Warmup
    for _ in range(config.warmup_operations):
        read_conn.execute_fetchall("SELECT * FROM users WHERE id = ?", [1])

    # Run concurrent tasks
    start_time = time.perf_counter()

    tasks = [
        worker_task(i, config, client, read_conn, results_collector)
        for i in range(config.concurrent_tasks_per_process)
    ]

    task_results = await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.perf_counter() - start_time

    # Aggregate results
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

    # Cleanup
    read_conn.close()

    return result


def worker_process(config: BenchmarkConfig, result_queue: Queue[ProcessResult]) -> None:
    """Entry point for worker processes."""
    try:
        # Each process gets its own event loop
        result = asyncio.run(run_worker_async(config))
        result_queue.put(result)
    except Exception as e:
        # Send error result
        result = ProcessResult(
            process_id=os.getpid(),
            total_reads=0,
            total_writes=0,
            errors=[f"Process error: {type(e).__name__}: {e}"],
        )
        result_queue.put(result)


def format_stats(values: list[float], unit: str = "ms") -> str:
    """Format statistics for a list of values."""
    if not values:
        return "N/A"

    return (
        f"min={min(values):.2f}{unit}, "
        f"max={max(values):.2f}{unit}, "
        f"mean={statistics.mean(values):.2f}{unit}, "
        f"median={statistics.median(values):.2f}{unit}, "
        f"p95={sorted(values)[int(len(values) * 0.95)]:.2f}{unit}"
    )


def run_benchmark(config: BenchmarkConfig) -> dict[str, Any]:
    """Run the multiprocess benchmark."""

    print(f"\n{'=' * 60}")
    print("MULTIPROCESS BENCHMARK")
    print(f"{'=' * 60}")
    print(f"Processes: {config.num_processes}")
    print(f"Operations per process: {config.operations_per_process}")
    print(f"Concurrent tasks per process: {config.concurrent_tasks_per_process}")
    print(f"Read/Write ratio: {config.read_write_ratio:.0%} / {1 - config.read_write_ratio:.0%}")
    print(f"Total expected operations: {config.num_processes * config.operations_per_process}")
    print(f"{'=' * 60}\n")

    # Setup database
    print("Setting up database...")
    setup_database(config.db_path)

    # Start writer server
    print("Starting writer server...")
    server = pasyn_sqlite_core.start_writer_server(config.db_path)
    config.socket_path = server.socket_path
    print(f"Writer server started at: {config.socket_path}")

    # Create result queue
    result_queue: Queue[ProcessResult] = mp.Queue()

    # Start worker processes
    print(f"\nStarting {config.num_processes} worker processes...")
    start_time = time.perf_counter()

    processes: list[mp.Process] = []
    for i in range(config.num_processes):
        p = mp.Process(target=worker_process, args=(config, result_queue))
        p.start()
        processes.append(p)
        print(f"  Started process {i + 1}/{config.num_processes} (PID: {p.pid})")

    # Wait for all processes to complete
    print("\nWaiting for processes to complete...")
    for p in processes:
        p.join()

    total_elapsed = time.perf_counter() - start_time

    # Collect results
    results: list[ProcessResult] = []
    while not result_queue.empty():
        results.append(result_queue.get())

    # Stop server
    server.stop()

    # Aggregate and display results
    print(f"\n{'=' * 60}")
    print("RESULTS")
    print(f"{'=' * 60}\n")

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

        print(f"Process {result.process_id}:")
        print(f"  Reads: {result.total_reads}, Writes: {result.total_writes}")
        print(f"  Elapsed: {result.elapsed_time_s:.2f}s")
        if result.errors:
            print(f"  Errors: {len(result.errors)}")

    print(f"\n{'-' * 60}")
    print("AGGREGATE STATISTICS")
    print(f"{'-' * 60}")
    print(f"Total elapsed time: {total_elapsed:.2f}s")
    print(f"Total operations: {total_reads + total_writes}")
    print(f"Total reads: {total_reads}")
    print(f"Total writes: {total_writes}")
    print(f"Overall throughput: {(total_reads + total_writes) / total_elapsed:.1f} ops/s")
    print(f"Read throughput: {total_reads / total_elapsed:.1f} reads/s")
    print(f"Write throughput: {total_writes / total_elapsed:.1f} writes/s")
    print()
    print(f"Read latencies:  {format_stats(all_read_latencies)}")
    print(f"Write latencies: {format_stats(all_write_latencies)}")

    if all_errors:
        print(f"\nErrors ({len(all_errors)} total):")
        for error in all_errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(all_errors) > 10:
            print(f"  ... and {len(all_errors) - 10} more")

    print(f"\n{'=' * 60}\n")

    return {
        "total_elapsed_s": total_elapsed,
        "total_operations": total_reads + total_writes,
        "total_reads": total_reads,
        "total_writes": total_writes,
        "throughput_ops_per_s": (total_reads + total_writes) / total_elapsed,
        "read_throughput": total_reads / total_elapsed,
        "write_throughput": total_writes / total_elapsed,
        "read_latencies_ms": all_read_latencies,
        "write_latencies_ms": all_write_latencies,
        "errors": all_errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Multiprocess benchmark for pasyn-sqlite")
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

        results = run_benchmark(config)

        if results["errors"]:
            print(f"Warning: {len(results['errors'])} errors occurred")
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
