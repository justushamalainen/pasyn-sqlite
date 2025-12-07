//! Basic example of using pasyn-sqlite-core
//!
//! Run with: cargo run --example basic

use pasyn_sqlite_core::{Connection, Result, Value};

/// Empty params constant for SQL statements without parameters
const NO_PARAMS: [Value; 0] = [];

fn main() -> Result<()> {
    println!("SQLite version: {}", pasyn_sqlite_core::sqlite_version());
    println!("Thread-safe: {}", pasyn_sqlite_core::sqlite_threadsafe());
    println!();

    // Open an in-memory database
    let conn = Connection::open_in_memory()?;

    // Create a table
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER
        )",
    )?;

    println!("Created users table");

    // Insert some data
    conn.execute(
        "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
        ["Alice", "alice@example.com", "30"],
    )?;

    conn.execute(
        "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
        [
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
            Value::Integer(25),
        ],
    )?;

    conn.execute(
        "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
        [
            Value::Text("Charlie".to_string()),
            Value::Null,
            Value::Integer(35),
        ],
    )?;

    println!("Inserted 3 users");
    println!("Last insert rowid: {}", conn.last_insert_rowid());
    println!();

    // Query all users
    println!("All users:");
    let mut stmt = conn.prepare("SELECT id, name, email, age FROM users ORDER BY id")?;
    while stmt.step()? {
        let id: i64 = stmt.column_int64(0);
        let name = stmt.column_text(1)?;
        let email = if stmt.column_is_null(2) {
            "N/A".to_string()
        } else {
            stmt.column_text(2)?.to_string()
        };
        let age: i32 = stmt.column_int(3);
        println!("  {} | {} | {} | {}", id, name, email, age);
    }
    println!();

    // Query with filter
    println!("Users over 27:");
    let result: Option<String> = conn.query_row(
        "SELECT name FROM users WHERE age > ?1 ORDER BY age DESC LIMIT 1",
        [27],
        |row| row.get(0),
    )?;
    if let Some(name) = result {
        println!("  Oldest user over 27: {}", name);
    }
    println!();

    // Transaction example
    println!("Transaction example:");
    {
        let tx = conn.begin_transaction()?;
        tx.connection()
            .execute("INSERT INTO users (name, age) VALUES (?1, ?2)", ["Dave", "40"])?;
        println!("  Inserted Dave (not committed yet)");

        // Commit the transaction
        tx.commit()?;
        println!("  Transaction committed");
    }

    // Count users
    let count: Option<i64> = conn.query_row("SELECT COUNT(*) FROM users", NO_PARAMS, |row| row.get(0))?;
    println!("Total users: {}", count.unwrap_or(0));

    // Memory stats
    println!();
    println!("SQLite memory used: {} bytes", pasyn_sqlite_core::memory_used());
    println!(
        "SQLite memory highwater: {} bytes",
        pasyn_sqlite_core::memory_highwater(false)
    );

    Ok(())
}
