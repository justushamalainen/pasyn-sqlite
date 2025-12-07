#!/usr/bin/env python3
"""Common utilities for tools scripts."""

import subprocess
import sys
from pathlib import Path

# Project root directory
ROOT_DIR = Path(__file__).parent.parent.resolve()
RUST_CORE_DIR = ROOT_DIR / "pasyn-sqlite-core"
RUST_POC_DIR = ROOT_DIR / "pasyn-await-poc"


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and print it."""
    cwd = cwd or ROOT_DIR
    print(f"+ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check)


def run_capture(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run a command and capture output."""
    cwd = cwd or ROOT_DIR
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def print_header(title: str) -> None:
    """Print a section header."""
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print()


def print_success(message: str) -> None:
    """Print a success message."""
    print(f"✓ {message}")


def print_error(message: str) -> None:
    """Print an error message."""
    print(f"✗ {message}", file=sys.stderr)


def check_command_exists(cmd: str) -> bool:
    """Check if a command exists in PATH."""
    result = run_capture(["which", cmd])
    return result.returncode == 0
