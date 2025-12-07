# Contributing to pasyn-sqlite

Thank you for your interest in contributing to pasyn-sqlite!

## Development Setup

### Prerequisites

- Python 3.9+
- Rust (stable, for building pasyn-sqlite-core)
- SQLite development libraries

#### Installing Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y libsqlite3-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**macOS:**
```bash
brew install sqlite3
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**Windows:**
```powershell
# Install Rust from https://rustup.rs
# SQLite is typically available via vcpkg or pre-built binaries
```

### Setting Up the Development Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/pasyn/pasyn-sqlite.git
   cd pasyn-sqlite
   ```

2. **Install all dependencies (recommended):**
   ```bash
   make install-all
   ```

   Or manually:
   ```bash
   # Install Python package with dev dependencies
   pip install -e ".[dev]"

   # Build and install the Rust core library
   pip install maturin patchelf
   cd pasyn-sqlite-core
   maturin build --features python --release
   pip install target/wheels/*.whl
   cd ..
   ```

### Running Tests

```bash
# Run all tests
make test

# Run only Python tests
make test-python

# Run only Rust tests
make test-rust
```

### Running Linters

```bash
# Run all linters
make lint

# Run only Python linters
make lint-python

# Run only Rust linters
make lint-rust
```

### Formatting Code

```bash
make format
```

### Running Benchmarks

```bash
make bench
```

## Project Structure

```
pasyn-sqlite/
├── pasyn_sqlite/           # Main Python package
│   ├── __init__.py
│   ├── pool.py             # Connection pool implementation
│   ├── pasyn_pool.py       # Advanced pool with work stealing
│   └── exceptions.py       # Custom exceptions
├── pasyn-sqlite-core/      # Rust core library with Python bindings
│   ├── src/                # Rust source code
│   ├── python/             # Python stub files
│   ├── Cargo.toml          # Rust dependencies
│   └── pyproject.toml      # Maturin build config
├── pasyn-await-poc/        # Proof of concept for native awaitables
├── benchmarks/             # Performance benchmarks
├── tests/                  # Python tests
├── Makefile                # Build and development commands
└── pyproject.toml          # Python project config
```

## Making Changes

1. Create a new branch for your changes
2. Make your changes
3. Run tests and linters: `make test && make lint`
4. Commit your changes with clear commit messages
5. Open a pull request

## Code Style

### Python
- Follow PEP 8
- Use type hints
- Use `ruff` for formatting and linting

### Rust
- Follow standard Rust conventions
- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting

## Release Process

Releases are automated via GitHub Actions when a new release is created on GitHub.

1. Update version numbers in:
   - `pyproject.toml`
   - `pasyn-sqlite-core/Cargo.toml`
   - `pasyn-sqlite-core/pyproject.toml`

2. Create a new release on GitHub

3. CI will automatically build and publish to PyPI

## Questions?

Open an issue on GitHub if you have questions or need help.
