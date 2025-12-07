.PHONY: help install install-dev install-rust build build-rust test test-python test-rust lint lint-python lint-rust format clean bench

PYTHON := python3
PIP := pip

help:
	@echo "pasyn-sqlite development commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install          Install the Python package"
	@echo "  make install-dev      Install with dev dependencies"
	@echo "  make install-rust     Build and install pasyn-sqlite-core (Rust)"
	@echo "  make install-all      Install everything (Python + Rust)"
	@echo ""
	@echo "Build:"
	@echo "  make build            Build Python package"
	@echo "  make build-rust       Build Rust wheel"
	@echo ""
	@echo "Test:"
	@echo "  make test             Run all tests"
	@echo "  make test-python      Run Python tests only"
	@echo "  make test-rust        Run Rust tests only"
	@echo ""
	@echo "Lint:"
	@echo "  make lint             Run all linters"
	@echo "  make lint-python      Run Python linters (ruff)"
	@echo "  make lint-rust        Run Rust linters (clippy, fmt)"
	@echo "  make format           Format all code"
	@echo ""
	@echo "Other:"
	@echo "  make bench            Run benchmarks"
	@echo "  make clean            Clean build artifacts"

# Installation
install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"
	$(PIP) install ruff mypy maturin patchelf

install-rust:
	cd pasyn-sqlite-core && maturin build --features python --release
	$(PIP) install pasyn-sqlite-core/target/wheels/*.whl --force-reinstall

install-all: install-dev install-rust

# Build
build:
	$(PYTHON) -m build

build-rust:
	cd pasyn-sqlite-core && maturin build --features python --release

# Testing
test: test-python test-rust

test-python:
	pytest tests/ -v

test-rust:
	cd pasyn-sqlite-core && cargo test --all-features

# Linting
lint: lint-python lint-rust

lint-python:
	ruff check .
	ruff format --check .

lint-rust:
	cd pasyn-sqlite-core && cargo fmt --check
	cd pasyn-sqlite-core && cargo clippy --all-features -- -D warnings

# Formatting
format:
	ruff format .
	ruff check --fix .
	cd pasyn-sqlite-core && cargo fmt

# Benchmarks
bench: install-all
	$(PYTHON) benchmarks/run_benchmarks.py

# Cleanup
clean:
	rm -rf build/ dist/ *.egg-info/
	rm -rf pasyn-sqlite-core/target/
	rm -rf pasyn-await-poc/target/
	rm -rf .pytest_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
