# pasyn-sqlite-core

High-performance SQLite bindings for Rust with optional Python bindings via PyO3.

## Features

- **Safe Rust API**: Idiomatic Rust wrappers around SQLite
- **Direct FFI bindings**: Low-level access to SQLite C API when needed
- **Python bindings**: Optional PyO3 bindings for seamless Python integration
- **Thread-safe**: Designed for concurrent access
- **Flexible linking**: Link to system SQLite or compile from source

## Installation

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
pasyn-sqlite-core = "0.1"
```

### Python

Build with maturin:

```bash
cd pasyn-sqlite-core
pip install maturin
maturin develop --features python
```

Or install from a wheel:

```bash
pip install pasyn-sqlite-core
```

## Quick Start (Rust)

```rust
use pasyn_sqlite_core::{Connection, Result};

fn main() -> Result<()> {
    // Open a database
    let conn = Connection::open_in_memory()?;

    // Create a table
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        [],
    )?;

    // Insert data
    conn.execute("INSERT INTO users (name) VALUES (?1)", ["Alice"])?;

    // Query data
    let name: Option<String> = conn.query_row(
        "SELECT name FROM users WHERE id = ?1",
        [1],
        |row| row.get(0),
    )?;

    println!("Name: {:?}", name);
    Ok(())
}
```

## Quick Start (Python)

```python
import pasyn_sqlite_core as sqlite

# Connect to database
conn = sqlite.connect(":memory:")

# Create table
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

# Insert data
conn.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])

# Query data
rows = conn.execute_fetchall("SELECT * FROM users")
print(rows)  # [(1, 'Alice')]

# With context manager
with sqlite.connect("mydb.sqlite") as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS data (value TEXT)")
    conn.execute("INSERT INTO data VALUES (?)", ["hello"])
    conn.commit()
```

## Features

### SQLite Compilation Options

- `system` (default): Link to system SQLite library
- `bundled`: Compile SQLite from source (requires `sqlite3.c` in `sqlite-src/`)
- `python`: Enable Python bindings via PyO3

### Example Feature Configuration

```toml
# Link to system SQLite (default)
pasyn-sqlite-core = "0.1"

# Compile SQLite from source
pasyn-sqlite-core = { version = "0.1", features = ["bundled"], default-features = false }
```

## API Reference

### Connection

- `Connection::open(path)` - Open a database file
- `Connection::open_in_memory()` - Open an in-memory database
- `Connection::open_with_flags(path, flags)` - Open with specific flags
- `conn.execute(sql, params)` - Execute SQL statement
- `conn.execute_batch(sql)` - Execute multiple statements
- `conn.prepare(sql)` - Prepare a statement
- `conn.query_row(sql, params, f)` - Query single row
- `conn.begin_transaction()` - Start a transaction

### Statement

- `stmt.execute(params)` - Execute with parameters
- `stmt.step()` - Step to next row
- `stmt.column_*()` - Get column values
- `stmt.bind(index, value)` - Bind parameter
- `stmt.reset()` - Reset for re-execution

### Value Types

SQLite values are represented as the `Value` enum:

```rust
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
```

Automatic conversions are provided for common Rust types.

## Thread Safety

This library is designed for thread-safe operation:

- `Connection` is `Send` but not `Sync` (one thread at a time per connection)
- Use connection pools for concurrent access
- SQLite is compiled with `SQLITE_THREADSAFE=1`

## Error Handling

All fallible operations return `Result<T, Error>`. The `Error` type includes:

- Error code from SQLite
- Extended error code (if available)
- Error message

```rust
match conn.execute("invalid sql", []) {
    Ok(_) => println!("Success"),
    Err(e) => {
        println!("Error: {}", e);
        println!("Code: {:?}", e.code);
    }
}
```

## License

MIT
