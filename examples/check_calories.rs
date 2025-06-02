use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open("tests/health_connect_export.db")?;
    
    // Check if active calories table exists
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='active_calories_burned_record_table'")?;
    let exists = stmt.exists([])?;
    
    if exists {
        // Count records in the active calories table
        let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM active_calories_burned_record_table")?;
        let count: i32 = count_stmt.query_row([], |row| row.get(0))?;
        println!("Active calories burned records: {}", count);
    } else {
        println!("Table active_calories_burned_record_table does not exist");
    }
    
    // Check if total calories table exists
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='total_calories_burned_record_table'")?;
    let exists = stmt.exists([])?;
    
    if exists {
        // Count records in the total calories table
        let mut count_stmt = conn.prepare("SELECT COUNT(*) FROM total_calories_burned_record_table")?;
        let count: i32 = count_stmt.query_row([], |row| row.get(0))?;
        println!("Total calories burned records: {}", count);
        
        // If there are records, show the schema
        if count > 0 {
            let mut schema_stmt = conn.prepare("PRAGMA table_info(total_calories_burned_record_table)")?;
            let schema_rows = schema_stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?, // column name
                    row.get::<_, String>(2)?, // column type
                ))
            })?;
            
            println!("Total calories table schema:");
            for schema_row in schema_rows {
                if let Ok((name, data_type)) = schema_row {
                    println!("  {} ({})", name, data_type);
                }
            }
            
            // Show a sample record with specific fields we know exist
            let mut sample_stmt = conn.prepare("SELECT start_time, end_time, energy, app_info_id FROM total_calories_burned_record_table LIMIT 1")?;
            let sample_data = sample_stmt.query_row([], |row| {
                Ok((
                    row.get::<_, i64>(0)?, // start_time
                    row.get::<_, i64>(1)?, // end_time
                    row.get::<_, f64>(2)?, // energy
                    row.get::<_, i64>(3)?, // app_info_id
                ))
            })?;
            
            println!("Sample record:");
            println!("  start_time: {}", sample_data.0);
            println!("  end_time: {}", sample_data.1);
            println!("  energy: {}", sample_data.2);
            println!("  app_info_id: {}", sample_data.3);
        }
    } else {
        println!("Table total_calories_burned_record_table does not exist");
    }
    
    Ok(())
}
