#!/usr/bin/env python3
"""Run linters for pasyn-sqlite."""

import argparse
import sys

from common import ROOT_DIR, RUST_CORE_DIR, print_error, print_header, print_success, run


def lint_python() -> bool:
    """Run Python linters (ruff)."""
    print_header("Running Python linters")
    success = True

    try:
        run(["ruff", "check", "."])
        print_success("ruff check passed")
    except Exception:
        print_error("ruff check failed")
        success = False

    try:
        run(["ruff", "format", "--check", "."])
        print_success("ruff format check passed")
    except Exception:
        print_error("ruff format check failed")
        success = False

    return success


def lint_rust() -> bool:
    """Run Rust linters (clippy, fmt)."""
    print_header("Running Rust linters")
    success = True

    try:
        run(["cargo", "fmt", "--check"], cwd=RUST_CORE_DIR)
        print_success("cargo fmt check passed")
    except Exception:
        print_error("cargo fmt check failed")
        success = False

    try:
        run(["cargo", "clippy", "--all-features", "--", "-D", "warnings"], cwd=RUST_CORE_DIR)
        print_success("cargo clippy passed")
    except Exception:
        print_error("cargo clippy failed")
        success = False

    return success


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pasyn-sqlite linters")
    parser.add_argument(
        "--python", action="store_true", help="Run Python linters only"
    )
    parser.add_argument(
        "--rust", action="store_true", help="Run Rust linters only"
    )
    args = parser.parse_args()

    # Default to running both if neither specified
    run_py = args.python or (not args.python and not args.rust)
    run_rs = args.rust or (not args.python and not args.rust)

    success = True

    if run_py:
        if not lint_python():
            success = False

    if run_rs:
        if not lint_rust():
            success = False

    if success:
        print_header("All linters passed")
    else:
        print_header("Some linters failed")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
