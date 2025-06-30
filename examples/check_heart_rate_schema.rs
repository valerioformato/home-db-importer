// Simple example to check heart rate table structure
use rusqlite::{Connection, Result as SqliteResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open("tests/health_connect_export.db")?;

    // Check heart rate record table structure
    println!("=== heart_rate_record_table structure ===");
    let mut stmt = conn.prepare("PRAGMA table_info(heart_rate_record_table)")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let col_name: String = row.get(1)?;
        let col_type: String = row.get(2)?;
        println!("Column: {} ({})", col_name, col_type);
    }

    // Check heart rate record series table structure
    println!("\n=== heart_rate_record_series_table structure ===");
    let mut stmt = conn.prepare("PRAGMA table_info(heart_rate_record_series_table)")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let col_name: String = row.get(1)?;
        let col_type: String = row.get(2)?;
        println!("Column: {} ({})", col_name, col_type);
    }

    // Check a few sample records
    println!("\n=== Sample heart rate records ===");
    let mut stmt = conn.prepare("SELECT hrs.epoch_millis, hrs.beats_per_minute, hrr.row_id 
                                 FROM heart_rate_record_series_table hrs
                                 LEFT JOIN heart_rate_record_table hrr ON hrs.heart_rate_record_id = hrr.row_id
                                 LIMIT 5")?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let epoch: i64 = row.get(0)?;
        let bpm: i64 = row.get(1)?;
        let record_id: Option<i64> = row.get(2).ok();
        println!("Epoch: {}, BPM: {}, Record ID: {:?}", epoch, bpm, record_id);
    }

    Ok(())
}
