#!/usr/bin/env python3
"""Run tests for pasyn-sqlite."""

import argparse
import sys

from common import RUST_CORE_DIR, print_error, print_header, print_success, run


def test_python(verbose: bool = False) -> bool:
    """Run Python tests."""
    print_header("Running Python tests")
    try:
        # Use sys.executable to ensure we use the same Python environment
        # where pasyn_sqlite_core is installed
        cmd = [sys.executable, "-m", "pytest", "tests/"]
        if verbose:
            cmd.append("-v")
        run(cmd)
        print_success("Python tests passed")
        return True
    except Exception as e:
        print_error(f"Python tests failed: {e}")
        return False


def test_rust() -> bool:
    """Run Rust tests."""
    print_header("Running Rust tests")
    try:
        run(["cargo", "test", "--all-features"], cwd=RUST_CORE_DIR)
        print_success("Rust tests passed")
        return True
    except Exception as e:
        print_error(f"Rust tests failed: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pasyn-sqlite tests")
    parser.add_argument("--python", action="store_true", help="Run Python tests only")
    parser.add_argument("--rust", action="store_true", help="Run Rust tests only")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    # Default to running both if neither specified
    run_py = args.python or (not args.python and not args.rust)
    run_rs = args.rust or (not args.python and not args.rust)

    success = True

    if run_py:
        if not test_python(verbose=args.verbose):
            success = False

    if run_rs:
        if not test_rust():
            success = False

    if success:
        print_header("All tests passed")
    else:
        print_header("Some tests failed")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
