# Claude Instructions for pasyn-sqlite

This file contains instructions for Claude (AI assistant) when working on this repository.

## Tools Directory Convention

**All commands that may be run multiple times should be Python scripts in the `tools/` directory.**

This includes:
- Build commands
- Test commands
- Lint/format commands
- Installation scripts
- Cleanup scripts
- Benchmark scripts

Do NOT use:
- Makefiles
- Shell scripts (unless truly necessary for platform-specific logic)
- npm scripts or other language-specific task runners

### Rationale

1. **Consistency**: Python is the primary language of this project
2. **Portability**: Python scripts work on all platforms without requiring make/bash
3. **Readability**: Python is more readable than shell scripts or Makefile syntax
4. **Extensibility**: Easy to add arguments, logging, error handling

### Available Tools

| Script | Purpose | Example Usage |
|--------|---------|---------------|
| `tools/install.py` | Install packages | `python tools/install.py --all` |
| `tools/build.py` | Build packages | `python tools/build.py --rust` |
| `tools/test.py` | Run tests | `python tools/test.py -v` |
| `tools/lint.py` | Run linters | `python tools/lint.py --python` |
| `tools/format.py` | Format code | `python tools/format.py` |
| `tools/bench.py` | Run benchmarks | `python tools/bench.py` |
| `tools/clean.py` | Clean artifacts | `python tools/clean.py` |

### Adding New Tools

When adding a new reusable command:

1. Create a new Python script in `tools/`
2. Import common utilities from `tools/common.py`
3. Use `argparse` for command-line arguments
4. Return appropriate exit codes (0 for success, non-zero for failure)
5. Add the script to the table above

Example template:

```python
#!/usr/bin/env python3
"""Description of what this tool does."""

import argparse
import sys

from common import ROOT_DIR, print_header, print_success, print_error, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Tool description")
    # Add arguments here
    args = parser.parse_args()

    # Tool logic here

    return 0  # or 1 on failure


if __name__ == "__main__":
    sys.exit(main())
```

## Project Structure

```
pasyn-sqlite/
├── pasyn_sqlite/           # Main Python package (pure Python)
├── pasyn-sqlite-core/      # Rust core library with Python bindings
├── pasyn-await-poc/        # Proof of concept (experimental)
├── benchmarks/             # Performance benchmarks
├── tests/                  # Python tests
├── tools/                  # Development scripts (see above)
├── .github/workflows/      # CI/CD configuration
└── pyproject.toml          # Python project configuration
```

## Common Tasks

### Before making changes
```bash
python tools/install.py --all  # Install everything
python tools/test.py           # Verify tests pass
```

### After making changes
```bash
python tools/format.py         # Format code
python tools/lint.py           # Check for issues
python tools/test.py           # Run tests
```

### Running benchmarks
```bash
python tools/bench.py          # Run full benchmark suite
```
