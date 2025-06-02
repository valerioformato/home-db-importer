use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    let conn = Connection::open("tests/health_connect_export.db")?;
    
    let tables_to_check = [
        "basal_metabolic_rate_record_table",
        "body_fat_record_table", 
        "exercise_session_record_table"
    ];
    
    for table in &tables_to_check {
        println!("=== {} ===", table);
        
        // Check if table exists
        let mut stmt = conn.prepare(&format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{}'", table))?;
        let exists = stmt.exists([])?;
        
        if exists {
            // Count records in the table
            let mut count_stmt = conn.prepare(&format!("SELECT COUNT(*) FROM {}", table))?;
            let count: i32 = count_stmt.query_row([], |row| row.get(0))?;
            println!("Records: {}", count);
            
            if count > 0 {
                // Show the schema
                let mut schema_stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
                let schema_rows = schema_stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?, // column name
                        row.get::<_, String>(2)?, // column type
                    ))
                })?;
                
                println!("Schema:");
                for schema_row in schema_rows {
                    if let Ok((name, data_type)) = schema_row {
                        println!("  {} ({})", name, data_type);
                    }
                }
                
                // Show a sample record
                let mut sample_stmt = conn.prepare(&format!("SELECT * FROM {} LIMIT 1", table))?;
                let column_count = sample_stmt.column_count();
                
                println!("Sample record:");
                let sample_data = sample_stmt.query_row([], |row| {
                    let mut values = Vec::new();
                    for i in 0..column_count {
                        match row.get::<_, rusqlite::types::Value>(i) {
                            Ok(val) => values.push(format!("{:?}", val)),
                            Err(_) => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values)
                })?;
                
                for (i, value) in sample_data.iter().enumerate() {
                    println!("  Column {}: {}", i, value);
                }
            }
        } else {
            println!("Table does not exist");
        }
        println!();
    }
    
    Ok(())
}
