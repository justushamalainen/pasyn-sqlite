"""
pasyn-sqlite-core - High-performance SQLite bindings for Python via Rust

This module provides a fast SQLite interface implemented in Rust with Python bindings.

Example usage:

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
"""

from .pasyn_sqlite_core import (
    # Classes
    Connection,
    Cursor,
    OpenFlags,
    SqliteError,
    # Functions
    connect,
    sqlite_version,
    sqlite_version_number,
    sqlite_threadsafe,
    memory_used,
    memory_highwater,
)

__all__ = [
    "Connection",
    "Cursor",
    "OpenFlags",
    "SqliteError",
    "connect",
    "sqlite_version",
    "sqlite_version_number",
    "sqlite_threadsafe",
    "memory_used",
    "memory_highwater",
]

__version__ = "0.1.0"
