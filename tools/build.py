#!/usr/bin/env python3
"""Build pasyn-sqlite packages."""

import argparse
import sys

from common import ROOT_DIR, RUST_CORE_DIR, print_error, print_header, print_success, run


def build_python() -> bool:
    """Build the Python package."""
    print_header("Building Python package")
    try:
        run(["pip", "install", "build"])
        run(["python", "-m", "build"])
        print_success("Python package built (see dist/)")
        return True
    except Exception as e:
        print_error(f"Failed to build Python package: {e}")
        return False


def build_rust(release: bool = True) -> bool:
    """Build the Rust core package wheel."""
    print_header("Building Rust core package")
    try:
        run(["pip", "install", "maturin", "patchelf"])
        cmd = ["maturin", "build", "--features", "python"]
        if release:
            cmd.append("--release")
        run(cmd, cwd=RUST_CORE_DIR)
        print_success("Rust wheel built (see pasyn-sqlite-core/target/wheels/)")
        return True
    except Exception as e:
        print_error(f"Failed to build Rust package: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build pasyn-sqlite packages")
    parser.add_argument(
        "--python", action="store_true", help="Build Python package only"
    )
    parser.add_argument(
        "--rust", action="store_true", help="Build Rust core package only"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Build Rust in debug mode"
    )
    args = parser.parse_args()

    # Default to building both if neither specified
    build_py = args.python or (not args.python and not args.rust)
    build_rs = args.rust or (not args.python and not args.rust)

    success = True

    if build_py:
        if not build_python():
            success = False

    if build_rs:
        if not build_rust(release=not args.debug):
            success = False

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
