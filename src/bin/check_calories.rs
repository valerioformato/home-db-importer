use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open("tests/health_connect_export.db")?;

    // Check if table exists
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='active_calories_burned_record_table'")?;
    let exists: bool = stmt
        .query_row([], |row| {
            Ok(row.get::<_, String>(0)? == "active_calories_burned_record_table")
        })
        .unwrap_or(false);

    if exists {
        // Count records in the table
        let mut count_stmt =
            conn.prepare("SELECT COUNT(*) FROM active_calories_burned_record_table")?;
        let count: i32 = count_stmt.query_row([], |row| row.get(0))?;
        println!("Active calories burned records: {}", count);
    } else {
        println!("Table active_calories_burned_record_table does not exist");
    }

    Ok(())
}
