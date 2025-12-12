#!/usr/bin/env python3
"""Run benchmarks for pasyn-sqlite."""

import argparse
import sys

from common import print_error, print_header, print_success, run


def run_benchmarks() -> bool:
    """Run the benchmark suite."""
    print_header("Running benchmarks")
    try:
        run(["python", "benchmarks/run_benchmarks.py"])
        print_success("Benchmarks completed")
        return True
    except Exception as e:
        print_error(f"Benchmarks failed: {e}")
        return False


def run_multiprocess_benchmark(
    processes: int, operations: int, concurrent: int, read_ratio: float
) -> bool:
    """Run the multiprocess benchmark (compares all implementations)."""
    print_header("Running multiprocess benchmark (all implementations)")
    try:
        cmd = [
            "python",
            "benchmarks/multiprocess_benchmark.py",
            "--processes",
            str(processes),
            "--operations",
            str(operations),
            "--concurrent",
            str(concurrent),
            "--read-ratio",
            str(read_ratio),
        ]
        run(cmd)
        print_success("Multiprocess benchmark completed")
        return True
    except Exception as e:
        print_error(f"Multiprocess benchmark failed: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pasyn-sqlite benchmarks")
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install dependencies before running (runs tools/install.py --all)",
    )
    parser.add_argument(
        "--multiprocess",
        "-m",
        action="store_true",
        help="Run multiprocess benchmark instead of standard benchmarks",
    )
    parser.add_argument(
        "--processes",
        "-p",
        type=int,
        default=4,
        help="Number of worker processes for multiprocess benchmark (default: 4)",
    )
    parser.add_argument(
        "--operations",
        "-o",
        type=int,
        default=1000,
        help="Operations per process for multiprocess benchmark (default: 1000)",
    )
    parser.add_argument(
        "--concurrent",
        "-c",
        type=int,
        default=10,
        help="Concurrent tasks per process for multiprocess benchmark (default: 10)",
    )
    parser.add_argument(
        "--read-ratio",
        "-r",
        type=float,
        default=0.8,
        help="Read ratio for multiprocess benchmark (0.0-1.0, default: 0.8)",
    )
    args = parser.parse_args()

    if args.install_deps:
        print_header("Installing dependencies")
        try:
            run(["python", "tools/install.py", "--all"])
        except Exception as e:
            print_error(f"Failed to install dependencies: {e}")
            return 1

    if args.multiprocess:
        if not run_multiprocess_benchmark(
            args.processes, args.operations, args.concurrent, args.read_ratio
        ):
            return 1
    else:
        if not run_benchmarks():
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
