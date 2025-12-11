#!/usr/bin/env python3
"""Format code in pasyn-sqlite."""

import argparse
import sys

from common import RUST_CORE_DIR, print_error, print_header, print_success, run


def format_python() -> bool:
    """Format Python code with ruff."""
    print_header("Formatting Python code")
    success = True

    try:
        run(["ruff", "format", "."])
        print_success("ruff format completed")
    except Exception:
        print_error("ruff format failed")
        success = False

    try:
        run(["ruff", "check", "--fix", "."])
        print_success("ruff check --fix completed")
    except Exception:
        print_error("ruff check --fix failed")
        success = False

    return success


def format_rust() -> bool:
    """Format Rust code with cargo fmt."""
    print_header("Formatting Rust code")
    try:
        run(["cargo", "fmt"], cwd=RUST_CORE_DIR)
        print_success("cargo fmt completed")
        return True
    except Exception:
        print_error("cargo fmt failed")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Format pasyn-sqlite code")
    parser.add_argument("--python", action="store_true", help="Format Python code only")
    parser.add_argument("--rust", action="store_true", help="Format Rust code only")
    args = parser.parse_args()

    # Default to formatting both if neither specified
    fmt_py = args.python or (not args.python and not args.rust)
    fmt_rs = args.rust or (not args.python and not args.rust)

    success = True

    if fmt_py:
        if not format_python():
            success = False

    if fmt_rs:
        if not format_rust():
            success = False

    if success:
        print_header("Formatting complete")
    else:
        print_header("Formatting completed with errors")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
