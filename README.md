# pasyn-sqlite

High-performance async SQLite with multiplexed writes and synchronous reads.

## Features

- **Single Writer Thread**: All write operations (INSERT, UPDATE, DELETE, etc.) are serialized through a dedicated writer server via Unix socket, ensuring SQLite's write consistency.
- **Synchronous Reads**: Read operations execute directly on a local read-only connection in the main thread - fast and efficient with no thread pool overhead.
- **Multiplexed Client**: Thread-safe client with automatic request batching for efficient write operations.
- **WAL Mode**: Uses WAL journal mode for better concurrent read/write performance.
- **Rust Core**: High-performance Rust implementation with Python bindings via PyO3.

## Installation

```bash
pip install pasyn-sqlite
```

Or for development:

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
import pasyn_sqlite_core as sqlite

# Start writer server for write operations
server = sqlite.start_writer_server("mydb.sqlite")

# Create multiplexed client for writes
client = sqlite.MultiplexedClient(server.socket_path)

# Create local read-only connection for reads
read_conn = sqlite.connect("mydb.sqlite", sqlite.OpenFlags.readonly())

# Create a table (write operation -> goes to writer server)
client.executescript('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT
    )
''')

# Insert data (write operation -> goes to writer server)
client.execute(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    ["Alice", "alice@example.com"]
)

# Query data (read operation -> synchronous, main thread)
rows = read_conn.execute_fetchall("SELECT * FROM users")
print(rows)  # [(1, 'Alice', 'alice@example.com')]

# Clean up
read_conn.close()
server.stop()
```

## API Reference

### Writer Server

#### `start_writer_server(database_path, socket_path=None)`

Start a writer server that handles all write operations via Unix socket.

- `database_path`: Path to SQLite database file
- `socket_path`: Optional path for Unix socket (auto-generated if not specified)

Returns a `WriterServerHandle` with methods:
- `socket_path` - Get the socket path
- `shutdown()` - Signal server to stop
- `join()` - Wait for server to stop
- `stop()` - Shutdown and wait

### Connection (for reads)

#### `connect(path, flags=None)`

Open a SQLite database connection for read operations.

- `path`: Path to SQLite database file
- `flags`: Optional `OpenFlags` (use `OpenFlags.readonly()` for read-only)

Returns a `Connection` with methods:
- `execute(sql, params=None)` - Execute SQL and return rows affected
- `execute_fetchall(sql, params=None)` - Execute SQL and return all rows
- `execute_fetchone(sql, params=None)` - Execute SQL and return first row
- `executescript(sql)` - Execute multiple SQL statements
- `begin()` / `commit()` / `rollback()` - Transaction control
- `close()` - Close the connection

### MultiplexedClient (for writes)

#### `MultiplexedClient(socket_path)`

Thread-safe client for sending write operations to the writer server.

- `socket_path`: Path to writer server Unix socket

Methods:
- `execute(sql, params=None)` - Execute SQL statement
- `execute_returning_rowid(sql, params=None)` - Execute and return last rowid
- `execute_many(sql, params_batch)` - Execute with multiple parameter sets
- `executescript(sql)` - Execute multiple SQL statements
- `begin()` / `commit()` / `rollback()` - Transaction control
- `ping()` - Check server is alive
- `shutdown_server()` - Request server shutdown

## Architecture

```
                    ┌─────────────────────┐
                    │     Your Code       │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                 │
    ┌─────────▼─────────┐           ┌──────────▼──────────┐
    │ MultiplexedClient │           │  Local Connection   │
    │   (for writes)    │           │   (for reads)       │
    │                   │           │                     │
    │  Thread-safe      │           │  Read-only          │
    │  Auto-batching    │           │  Synchronous        │
    │  GIL-releasing    │           │  Main thread        │
    └─────────┬─────────┘           └──────────┬──────────┘
              │                                 │
              │ Unix Socket                     │ Direct
              │                                 │
    ┌─────────▼─────────┐                       │
    │   Writer Server   │                       │
    │   (single thread) │                       │
    │                   │                       │
    │  - INSERT         │                       │
    │  - UPDATE         │                       │
    │  - DELETE         │                       │
    │  - CREATE/DROP    │                       │
    │  - COMMIT/ROLLBACK│                       │
    └─────────┬─────────┘                       │
              │                                 │
              └────────────────┬────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   SQLite Database   │
                    │   (WAL mode)        │
                    └─────────────────────┘
```

## Thread Safety

- **Write operations**: Serialized through the single writer server thread
- **Read operations**: Execute synchronously on the calling thread via local read-only connection
- **SQLite WAL mode**: Allows concurrent reads even during writes
- **MultiplexedClient**: Thread-safe with lock-free request submission and automatic batching

## Exceptions

`pasyn_sqlite` re-exports all `sqlite3` exceptions and adds:

- `SqliteError` - Base exception for SQLite errors (from Rust bindings)
- `PoolError` - Base exception for connection pool errors
- `PoolClosedError` - Connection pool has been closed
- `ConnectionClosedError` - Connection has been closed

## Requirements

- Python 3.9+
- Rust toolchain (for building from source)

## License

MIT
