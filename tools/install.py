#!/usr/bin/env python3
"""Install pasyn-sqlite packages."""

import argparse
import glob
import sys

from common import ROOT_DIR, RUST_CORE_DIR, print_error, print_header, print_success, run


def install_python(dev: bool = False) -> bool:
    """Install the Python package."""
    print_header("Installing Python package")
    try:
        if dev:
            run(["pip", "install", "-e", ".[dev]"])
        else:
            run(["pip", "install", "-e", "."])
        print_success("Python package installed")
        return True
    except Exception as e:
        print_error(f"Failed to install Python package: {e}")
        return False


def install_rust_core() -> bool:
    """Build and install the Rust core package."""
    print_header("Building and installing Rust core package")

    # Check for maturin
    try:
        run(["pip", "install", "maturin", "patchelf"])
    except Exception as e:
        print_error(f"Failed to install maturin: {e}")
        return False

    # Build wheel
    try:
        run(["maturin", "build", "--features", "python", "--release"], cwd=RUST_CORE_DIR)
    except Exception as e:
        print_error(f"Failed to build Rust package: {e}")
        return False

    # Find and install wheel
    wheels = glob.glob(str(RUST_CORE_DIR / "target" / "wheels" / "*.whl"))
    if not wheels:
        print_error("No wheel found after build")
        return False

    try:
        run(["pip", "install", wheels[0], "--force-reinstall"])
        print_success("Rust core package installed")
        return True
    except Exception as e:
        print_error(f"Failed to install wheel: {e}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Install pasyn-sqlite packages")
    parser.add_argument(
        "--dev", action="store_true", help="Install with development dependencies"
    )
    parser.add_argument(
        "--rust", action="store_true", help="Also build and install Rust core package"
    )
    parser.add_argument(
        "--all", action="store_true", help="Install everything (Python + Rust)"
    )
    args = parser.parse_args()

    success = True

    # Install Python package
    if not install_python(dev=args.dev or args.all):
        success = False

    # Install Rust core if requested
    if args.rust or args.all:
        if not install_rust_core():
            success = False

    if success:
        print_header("Installation complete")
        return 0
    else:
        print_header("Installation completed with errors")
        return 1


if __name__ == "__main__":
    sys.exit(main())
