#!/usr/bin/env python3
"""Run benchmarks for pasyn-sqlite."""

import argparse
import sys

from common import ROOT_DIR, print_error, print_header, print_success, run


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pasyn-sqlite benchmarks")
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install dependencies before running (runs tools/install.py --all)",
    )
    args = parser.parse_args()

    if args.install_deps:
        print_header("Installing dependencies")
        try:
            run(["python", "tools/install.py", "--all"])
        except Exception as e:
            print_error(f"Failed to install dependencies: {e}")
            return 1

    if not run_benchmarks():
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
