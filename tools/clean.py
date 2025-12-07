#!/usr/bin/env python3
"""Clean build artifacts from pasyn-sqlite."""

import argparse
import shutil
import sys
from pathlib import Path

from common import ROOT_DIR, RUST_CORE_DIR, RUST_POC_DIR, print_header, print_success


def clean_python() -> None:
    """Clean Python build artifacts."""
    print_header("Cleaning Python artifacts")

    dirs_to_remove = [
        ROOT_DIR / "build",
        ROOT_DIR / "dist",
        ROOT_DIR / ".pytest_cache",
        ROOT_DIR / ".ruff_cache",
        ROOT_DIR / ".mypy_cache",
    ]

    # Find all egg-info directories
    dirs_to_remove.extend(ROOT_DIR.glob("*.egg-info"))

    # Find all __pycache__ directories
    dirs_to_remove.extend(ROOT_DIR.rglob("__pycache__"))

    for d in dirs_to_remove:
        if d.exists():
            print(f"  Removing {d.relative_to(ROOT_DIR)}")
            shutil.rmtree(d)

    # Remove .pyc files
    for f in ROOT_DIR.rglob("*.pyc"):
        print(f"  Removing {f.relative_to(ROOT_DIR)}")
        f.unlink()

    print_success("Python artifacts cleaned")


def clean_rust() -> None:
    """Clean Rust build artifacts."""
    print_header("Cleaning Rust artifacts")

    targets = [
        RUST_CORE_DIR / "target",
        RUST_POC_DIR / "target",
    ]

    for target in targets:
        if target.exists():
            print(f"  Removing {target.relative_to(ROOT_DIR)}")
            shutil.rmtree(target)

    print_success("Rust artifacts cleaned")


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean pasyn-sqlite build artifacts")
    parser.add_argument(
        "--python", action="store_true", help="Clean Python artifacts only"
    )
    parser.add_argument(
        "--rust", action="store_true", help="Clean Rust artifacts only"
    )
    args = parser.parse_args()

    # Default to cleaning both if neither specified
    clean_py = args.python or (not args.python and not args.rust)
    clean_rs = args.rust or (not args.python and not args.rust)

    if clean_py:
        clean_python()

    if clean_rs:
        clean_rust()

    print_header("Clean complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
