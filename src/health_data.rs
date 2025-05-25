use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{Connection, Result, Row};
use std::path::Path;

/// Represents a client for reading Health Connect data from SQLite
pub struct HealthDataReader {
    db_path: String,
}

impl HealthDataReader {
    /// Creates a new HealthDataReader
    pub fn new(db_path: &str) -> Self {
        HealthDataReader {
            db_path: db_path.to_string(),
        }
    }

    /// Checks if the database file exists
    pub fn db_exists(&self) -> bool {
        Path::new(&self.db_path).exists()
    }

    /// Opens a connection to the database
    pub fn open_connection(&self) -> Result<Connection> {
        Connection::open(&self.db_path)
    }

    /// Validates the database structure
    pub fn validate_db(&self) -> Result<String, Box<dyn std::error::Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;

        // Get a list of tables in the database
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>>>()?;

        let mut output = String::new();
        output.push_str(&format!("Database: {}\n", self.db_path));
        output.push_str(&format!("Found {} tables:\n", tables.len()));

        for table in &tables {
            output.push_str(&format!("  - {}\n", table));
        }

        Ok(output)
    }
}
