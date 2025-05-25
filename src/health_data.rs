use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{Connection, Result as SqliteResult, Row};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

/// Represents a client for reading Health Connect data from SQLite
pub struct HealthDataReader {
    db_path: String,
}

/// Represents a health data record extracted from SQLite
#[derive(Debug, Clone)]
pub struct HealthRecord {
    #[allow(dead_code)] // Used by InfluxClient when converting to data points
    pub record_type: String, // Type of health record (e.g., "HeartRate", "Steps")
    pub timestamp: DateTime<Utc>, // When the measurement was taken
    pub value: f64,               // The measurement value
    pub metadata: HashMap<String, String>, // Additional data like device info, etc.
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
    pub fn open_connection(&self) -> SqliteResult<Connection> {
        Connection::open(&self.db_path)
    }

    /// Validates the database structure
    pub fn validate_db(&self) -> Result<String, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;

        // Get a list of tables in the database
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<SqliteResult<Vec<String>>>()?;

        let mut output = String::new();
        output.push_str(&format!("Database: {}\n", self.db_path));
        output.push_str(&format!("Found {} tables:\n", tables.len()));

        for table in &tables {
            output.push_str(&format!("  - {}\n", table));

            // Get column info for each table
            if let Ok(mut pragma_stmt) = conn.prepare(&format!("PRAGMA table_info({})", table)) {
                let columns = pragma_stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?, // column name
                        row.get::<_, String>(2)?, // column type
                    ))
                })?;

                for (name, col_type) in columns.flatten() {
                    output.push_str(&format!("      {} ({})\n", name, col_type));
                }
            }

            // Get sample record count
            if let Ok(mut count_stmt) = conn.prepare(&format!("SELECT COUNT(*) FROM {}", table)) {
                if let Ok(count) = count_stmt.query_row([], |row| row.get::<_, i64>(0)) {
                    output.push_str(&format!("      Records: {}\n", count));
                }
            }
        }

        Ok(output)
    }

    /// Retrieves heart rate data after a specific timestamp
    pub fn get_heart_rate_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Updated query based on the actual schema (heart_rate_record_table and heart_rate_record_series_table)
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT hrs.epoch_millis, hrs.beats_per_minute, ai.app_name 
                 FROM heart_rate_record_series_table hrs
                 JOIN heart_rate_record_table hr ON hrs.parent_key = hr.row_id
                 LEFT JOIN application_info_table ai ON hr.app_info_id = ai.row_id
                 WHERE hrs.epoch_millis > ? 
                 ORDER BY hrs.epoch_millis ASC"
                    .to_string()
            }
            None => "SELECT hrs.epoch_millis, hrs.beats_per_minute, ai.app_name
                 FROM heart_rate_record_series_table hrs
                 JOIN heart_rate_record_table hr ON hrs.parent_key = hr.row_id
                 LEFT JOIN application_info_table ai ON hr.app_info_id = ai.row_id
                 ORDER BY hrs.epoch_millis ASC"
                .to_string(),
        };

        let mut stmt = match conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                // If the table doesn''t exist yet, return empty results
                if e.to_string().contains("no such table") {
                    return Ok(Vec::new());
                }
                return Err(Box::new(e));
            }
        };

        let mut rows = match since {
            Some(timestamp) => {
                let unix_timestamp = timestamp.timestamp_millis();
                stmt.query([unix_timestamp])?
            }
            None => stmt.query([])?,
        };

        while let Some(row_result) = rows.next()? {
            match self.map_heart_rate_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading heart rate record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a HeartRate HealthRecord
    fn map_heart_rate_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let time_millis: i64 = row.get(0)?;
        let value: i64 = row.get(1)?; // beats_per_minute is an INTEGER in the schema
        let app_name: String = row.get(2).unwrap_or_else(|_| "unknown".to_string());

        let timestamp = Utc
            .timestamp_millis_opt(time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);

        Ok(HealthRecord {
            record_type: "HeartRate".to_string(),
            timestamp,
            value: value as f64, // Convert INTEGER to f64
            metadata,
        })
    }

    /// Retrieves step count data after a specific timestamp
    pub fn get_steps_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Updated query based on the actual schema (steps_record_table)
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT start_time, count, ai.app_name
                 FROM steps_record_table sr
                 LEFT JOIN application_info_table ai ON sr.app_info_id = ai.row_id
                 WHERE start_time > ? 
                 ORDER BY start_time ASC"
                    .to_string()
            }
            None => "SELECT start_time, count, ai.app_name
                 FROM steps_record_table sr
                 LEFT JOIN application_info_table ai ON sr.app_info_id = ai.row_id
                 ORDER BY start_time ASC"
                .to_string(),
        };

        let mut stmt = match conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                // If the table doesn''t exist yet, return empty results
                if e.to_string().contains("no such table") {
                    return Ok(Vec::new());
                }
                return Err(Box::new(e));
            }
        };

        let mut rows = match since {
            Some(timestamp) => {
                let unix_timestamp = timestamp.timestamp_millis();
                stmt.query([unix_timestamp])?
            }
            None => stmt.query([])?,
        };

        while let Some(row_result) = rows.next()? {
            match self.map_steps_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading steps record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a Steps HealthRecord
    fn map_steps_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let time_millis: i64 = row.get(0)?;
        let value: i64 = row.get(1)?; // count is an INTEGER in the schema
        let app_name: String = row.get(2).unwrap_or_else(|_| "unknown".to_string());

        let timestamp = Utc
            .timestamp_millis_opt(time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);

        Ok(HealthRecord {
            record_type: "Steps".to_string(),
            timestamp,
            value: value as f64, // Convert INTEGER to f64
            metadata,
        })
    }

    /// Retrieves sleep data after a specific timestamp
    pub fn get_sleep_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for sleep records based on sleep_session_record_table and sleep_stages_table
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT ss.start_time, ss.end_time, st.stage_type, ai.app_name
                 FROM sleep_session_record_table ss
                 JOIN sleep_stages_table st ON st.parent_key = ss.row_id
                 LEFT JOIN application_info_table ai ON ss.app_info_id = ai.row_id
                 WHERE ss.start_time > ? 
                 ORDER BY ss.start_time ASC, st.stage_start_time ASC"
                    .to_string()
            }
            None => "SELECT ss.start_time, ss.end_time, st.stage_type, ai.app_name
                 FROM sleep_session_record_table ss
                 JOIN sleep_stages_table st ON st.parent_key = ss.row_id
                 LEFT JOIN application_info_table ai ON ss.app_info_id = ai.row_id
                 ORDER BY ss.start_time ASC, st.stage_start_time ASC"
                .to_string(),
        };

        let mut stmt = match conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                // If the table doesn't exist yet, return empty results
                if e.to_string().contains("no such table") {
                    return Ok(Vec::new());
                }
                return Err(Box::new(e));
            }
        };

        let mut rows = match since {
            Some(timestamp) => {
                let unix_timestamp = timestamp.timestamp_millis();
                stmt.query([unix_timestamp])?
            }
            None => stmt.query([])?,
        };

        while let Some(row_result) = rows.next()? {
            match self.map_sleep_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading sleep record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a Sleep HealthRecord
    fn map_sleep_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let start_time_millis: i64 = row.get(0)?;
        let end_time_millis: i64 = row.get(1)?;
        let stage_type: i64 = row.get(2)?;
        let app_name: String = row.get(3).unwrap_or_else(|_| "unknown".to_string());

        let start_timestamp = Utc
            .timestamp_millis_opt(start_time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        let end_timestamp = Utc
            .timestamp_millis_opt(end_time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        // Calculate duration in minutes as the value
        let duration_millis = end_time_millis - start_time_millis;
        let duration_minutes = duration_millis as f64 / (1000.0 * 60.0);

        // Convert stage type integer to descriptive string
        let stage_description = match stage_type {
            1 => "AWAKE",
            2 => "SLEEPING",
            3 => "OUT_OF_BED",
            4 => "LIGHT",
            5 => "DEEP",
            6 => "REM",
            _ => "UNKNOWN",
        };

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("stage".to_string(), stage_description.to_string());
        metadata.insert("stage_type".to_string(), stage_type.to_string());
        metadata.insert("start_time".to_string(), start_timestamp.to_rfc3339());
        metadata.insert("end_time".to_string(), end_timestamp.to_rfc3339());

        Ok(HealthRecord {
            record_type: "Sleep".to_string(),
            timestamp: start_timestamp, // Use start time as the primary timestamp
            value: duration_minutes,    // Duration in minutes
            metadata,
        })
    }

    /// Retrieves weight data after a specific timestamp
    pub fn get_weight_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for weight records
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT wr.time, wr.weight, ai.app_name
                 FROM weight_record_table wr
                 LEFT JOIN application_info_table ai ON wr.app_info_id = ai.row_id
                 WHERE wr.time > ? 
                 ORDER BY wr.time ASC"
                    .to_string()
            }
            None => "SELECT wr.time, wr.weight, ai.app_name
                 FROM weight_record_table wr
                 LEFT JOIN application_info_table ai ON wr.app_info_id = ai.row_id
                 ORDER BY wr.time ASC"
                .to_string(),
        };

        let mut stmt = match conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                // If the table doesn't exist yet, return empty results
                if e.to_string().contains("no such table") {
                    return Ok(Vec::new());
                }
                return Err(Box::new(e));
            }
        };

        let mut rows = match since {
            Some(timestamp) => {
                let unix_timestamp = timestamp.timestamp_millis();
                stmt.query([unix_timestamp])?
            }
            None => stmt.query([])?,
        };

        while let Some(row_result) = rows.next()? {
            match self.map_weight_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading weight record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a Weight HealthRecord
    fn map_weight_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let time_millis: i64 = row.get(0)?;
        let weight_value: f64 = row.get(1)?;
        let app_name: String = row.get(2).unwrap_or_else(|_| "unknown".to_string());

        let timestamp = Utc
            .timestamp_millis_opt(time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("unit".to_string(), "g".to_string());

        Ok(HealthRecord {
            record_type: "Weight".to_string(),
            timestamp,
            value: weight_value,
            metadata,
        })
    }

    /// Gets all available health data since a specific timestamp
    pub fn get_all_health_data_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<HashMap<String, Vec<HealthRecord>>, Box<dyn Error>> {
        let mut all_data = HashMap::new();

        // Get heart rate data
        match self.get_heart_rate_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("HeartRate".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching heart rate data: {}", e),
        }

        // Get steps data
        match self.get_steps_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("Steps".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching steps data: {}", e),
        }

        // Get sleep data
        match self.get_sleep_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("Sleep".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching sleep data: {}", e),
        }

        // Get weight data
        match self.get_weight_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("Weight".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching weight data: {}", e),
        }

        // Add more data types as needed

        Ok(all_data)
    }
}
