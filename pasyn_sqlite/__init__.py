"""
pasyn-sqlite: Async SQLite with thread pool.

Single writer thread, multiple reader threads with work-stealing
for optimal SQLite performance in async Python applications.

Example:
    import asyncio
    import pasyn_sqlite

    async def main():
        async with await pasyn_sqlite.connect("mydb.sqlite") as conn:
            # Create a table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            ''')
            await conn.commit()

            # Insert data (goes to writer thread)
            await conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            await conn.commit()

            # Query data (goes to reader threads with work-stealing)
            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
            print(rows)

    asyncio.run(main())
"""

from .connection import Connection, connect
from .cursor import Cursor
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
    Warning,
)
from .pool import ThreadPool

__version__ = "0.1.0"

__all__ = [
    # Main API
    "connect",
    "Connection",
    "Cursor",
    "ThreadPool",
    # Exceptions
    "PoolError",
    "PoolClosedError",
    "ConnectionClosedError",
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
