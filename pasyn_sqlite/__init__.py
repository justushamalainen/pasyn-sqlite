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
        async with pool.bound_connection() as conn:
            await conn.execute("BEGIN")
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Charlie",))
            await conn.execute("COMMIT")

        await pool.close()

    asyncio.run(main())
"""

# New Pool API
from .pasyn_pool import (
    BoundConnection,
    Cursor,
    PasynPool,
    create_pool,
)

# Legacy API (kept for compatibility)
from .connection import Connection, connect

from .exceptions import (
    ConnectionClosedError,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NoActiveTransactionError,
    NotSupportedError,
    OperationalError,
    PoolClosedError,
    PoolError,
    ProgrammingError,
    TransactionAlreadyActiveError,
    TransactionCommandError,
    TransactionError,
    Warning,
)
from .pool import ThreadPool
from .transactions import IsolationLevel, Savepoint, Transaction, TransactionContext

__version__ = "0.1.0"

__all__ = [
    # New Pool API
    "create_pool",
    "PasynPool",
    "BoundConnection",
    "Cursor",
    # Legacy API
    "connect",
    "Connection",
    "ThreadPool",
    # Transactions (legacy)
    "Transaction",
    "TransactionContext",
    "Savepoint",
    "IsolationLevel",
    # Exceptions
    "PoolError",
    "PoolClosedError",
    "ConnectionClosedError",
    "TransactionError",
    "TransactionAlreadyActiveError",
    "NoActiveTransactionError",
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
