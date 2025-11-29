# pasyn-sqlite

Async SQLite with thread pool - single writer, multiple readers with work stealing.

## Features

- **Single Writer Thread**: All write operations (INSERT, UPDATE, DELETE, etc.) are serialized through a dedicated writer thread, ensuring SQLite's write consistency.
- **Multiple Reader Threads**: Configurable number of reader threads (default: 3) handle SELECT queries concurrently.
- **Work Stealing**: Reader threads use a work-stealing algorithm for load balancing - idle readers steal tasks from busy readers.
- **Async/Await API**: Mirrors the standard library `sqlite3` interface but with async methods.
- **WAL Mode**: Automatically enables WAL journal mode for better concurrent performance.

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
import asyncio
import pasyn_sqlite

async def main():
    # Connect to database (creates file if it doesn't exist)
    async with await pasyn_sqlite.connect("mydb.sqlite") as conn:
        # Create a table (write operation -> goes to writer thread)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        ''')
        await conn.commit()

        # Insert data (write operation -> goes to writer thread)
        await conn.execute(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            ("Alice", "alice@example.com")
        )
        await conn.commit()

        # Query data (read operation -> goes to reader thread pool)
        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print(rows)

asyncio.run(main())
```

## API Reference

### `connect(database, *, num_readers=3, **kwargs)`

Create an async database connection.

- `database`: Path to SQLite database file or `:memory:` for in-memory database
- `num_readers`: Number of reader threads (default: 3)
- `**kwargs`: Additional arguments passed to `sqlite3.connect()`

### `Connection`

Async connection that mirrors `sqlite3.Connection`:

- `await conn.execute(sql, parameters=())` - Execute SQL and return cursor
- `await conn.executemany(sql, parameters)` - Execute SQL with multiple parameter sets
- `await conn.executescript(script)` - Execute multiple SQL statements
- `await conn.commit()` - Commit current transaction
- `await conn.rollback()` - Rollback current transaction
- `await conn.close()` - Close the connection
- `await conn.cursor()` - Create a new cursor

### `Cursor`

Async cursor that mirrors `sqlite3.Cursor`:

- `await cursor.execute(sql, parameters=())` - Execute SQL
- `await cursor.executemany(sql, parameters)` - Execute with multiple parameter sets
- `await cursor.fetchone()` - Fetch next row
- `await cursor.fetchmany(size=None)` - Fetch next `size` rows
- `await cursor.fetchall()` - Fetch all remaining rows
- `cursor.description` - Column descriptions
- `cursor.rowcount` - Number of affected rows
- `cursor.lastrowid` - Last inserted row ID

### Async Iteration

Cursors support async iteration:

```python
cursor = await conn.execute("SELECT * FROM users")
async for row in cursor:
    print(row)
```

### Context Managers

Both connections and cursors work as async context managers:

```python
async with await pasyn_sqlite.connect("db.sqlite") as conn:
    async with await conn.execute("SELECT 1") as cursor:
        row = await cursor.fetchone()
```

## Architecture

```
                    ┌─────────────────────┐
                    │    Your Async Code  │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │    pasyn_sqlite     │
                    │   Connection/Cursor │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                 │
    ┌─────────▼─────────┐           ┌──────────▼──────────┐
    │   Writer Thread   │           │   Reader Pool       │
    │   (single)        │           │   (configurable)    │
    │                   │           │                     │
    │  - INSERT         │           │  Reader 0 ◄──┐      │
    │  - UPDATE         │           │  Reader 1 ◄──┼─ Work│
    │  - DELETE         │           │  Reader 2 ◄──┘  Stealing
    │  - CREATE/DROP    │           │                     │
    │  - COMMIT/ROLLBACK│           │  SELECT queries     │
    └─────────┬─────────┘           └──────────┬──────────┘
              │                                 │
              └────────────────┬────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   SQLite Database   │
                    │   (WAL mode)        │
                    └─────────────────────┘
```

## Work Stealing

When a reader thread's local queue is empty, it attempts to steal work from other readers before checking the shared queue. This ensures better load distribution and reduces idle time.

```
Reader 0: [Task A, Task B, Task C]  ◄── Owner takes from front
Reader 1: [Task D]
Reader 2: []  ─────────────────────────► Steals from back of Reader 0's queue
```

## Thread Safety

- Write operations are always serialized through the single writer thread
- Read operations can run concurrently across multiple reader threads
- SQLite WAL mode allows concurrent reads even during writes
- All connection setup includes `busy_timeout` to handle temporary locks

## Exceptions

`pasyn_sqlite` re-exports all `sqlite3` exceptions and adds:

- `PoolError` - Base exception for pool errors
- `PoolClosedError` - Pool has been closed
- `ConnectionClosedError` - Connection has been closed

## Requirements

- Python 3.9+
- No external dependencies (uses only standard library)

## License

MIT
