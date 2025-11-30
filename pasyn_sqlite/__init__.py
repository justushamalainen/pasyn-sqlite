"""
pasyn-sqlite: Async SQLite with thread pool.

Single writer thread, multiple reader threads with work-stealing
for optimal SQLite performance in async Python applications.

Example:
    import asyncio
    import pasyn_sqlite

    async def main():
        # Create a pool
        pool = await pasyn_sqlite.create_pool("mydb.sqlite")

        # Simple queries - auto-routed to writer/reader
        await pool.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')

        # Insert data (goes to writer thread)
        await pool.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))

        # Query data (goes to reader threads)
        cursor = await pool.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print(rows)

        # Transactions - use bound_connection
        async with await pool.bound_connection() as conn:
            await conn.execute("BEGIN")
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Charlie",))
            await conn.execute("COMMIT")

        await pool.close()

    asyncio.run(main())
"""

# Pool API
from .pasyn_pool import (
    BoundConnection,
    Cursor,
    PasynPool,
    create_pool,
)

from .exceptions import (
    ConnectionClosedError,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    PoolClosedError,
    PoolError,
    ProgrammingError,
    TransactionCommandError,
    TransactionError,
    Warning,
)
from .pool import ThreadPool

__version__ = "0.1.0"

__all__ = [
    # Pool API
    "create_pool",
    "PasynPool",
    "BoundConnection",
    "Cursor",
    "ThreadPool",
    # Exceptions
    "PoolError",
    "PoolClosedError",
    "ConnectionClosedError",
    "TransactionError",
    "TransactionCommandError",
    # Re-exported sqlite3 exceptions
    "Error",
    "Warning",
    "DatabaseError",
    "DataError",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    # Version
    "__version__",
]
