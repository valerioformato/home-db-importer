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

        // Check for specific tables and their record counts
        let tables_to_check = [
            "heart_rate_record_table",
            "steps_record_table",
            "sleep_session_record_table",
            "weight_record_table",
            "active_calories_burned_record_table",
            "total_calories_burned_record_table",
            "basal_metabolic_rate_record_table",
            "body_fat_record_table",
            "exercise_session_record_table",
        ];

        for table in &tables_to_check {
            output.push_str(&format!("  - {}\n", table));

            // Get sample record count
            if let Ok(mut count_stmt) = conn.prepare(&format!("SELECT COUNT(*) FROM {}", table)) {
                if let Ok(count) = count_stmt.query_row([], |row| row.get::<_, i64>(0)) {
                    output.push_str(&format!("      Records: {}\n", count));
                }
            } else {
                output.push_str("      Table does not exist or cannot be accessed\n");
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
                Ok(stage_records) => {
                    // Extend the records vec with all the records for this sleep stage
                    records.extend(stage_records);
                }
                Err(e) => eprintln!("Error reading sleep record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to multiple Sleep HealthRecords (start and end points)
    fn map_sleep_row(&self, row: &Row) -> SqliteResult<Vec<HealthRecord>> {
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

        // Numeric value for the sleep stage (useful for visualization in Grafana)
        let stage_value = match stage_type {
            1 => 0.0,  // AWAKE
            2 => 1.0,  // SLEEPING (generic)
            3 => 0.0,  // OUT_OF_BED
            4 => 2.0,  // LIGHT
            5 => 3.0,  // DEEP
            6 => 4.0,  // REM
            _ => -1.0, // UNKNOWN
        };

        let mut results = Vec::new();

        // Create metadata for the start point
        let mut start_metadata = HashMap::new();
        start_metadata.insert("app_name".to_string(), app_name.clone());
        start_metadata.insert("stage".to_string(), stage_description.to_string());
        start_metadata.insert("stage_type".to_string(), stage_type.to_string());
        start_metadata.insert("event_type".to_string(), "start".to_string());
        start_metadata.insert("duration_minutes".to_string(), duration_minutes.to_string());

        // Start point - Main data point with stage value
        results.push(HealthRecord {
            record_type: "Sleep".to_string(),
            timestamp: start_timestamp,
            value: stage_value, // Use stage value for visualization
            metadata: start_metadata,
        });

        // Create metadata for the end point
        let mut end_metadata = HashMap::new();
        end_metadata.insert("app_name".to_string(), app_name.clone());
        end_metadata.insert("stage".to_string(), stage_description.to_string());
        end_metadata.insert("stage_type".to_string(), stage_type.to_string());
        end_metadata.insert("event_type".to_string(), "end".to_string());
        end_metadata.insert("duration_minutes".to_string(), duration_minutes.to_string());

        // End point
        results.push(HealthRecord {
            record_type: "Sleep".to_string(),
            timestamp: end_timestamp,
            value: 0.0, // End of this sleep stage
            metadata: end_metadata,
        });

        // Add a sleep session record with duration for Grafana
        let mut duration_metadata = HashMap::new();
        duration_metadata.insert("app_name".to_string(), app_name.clone());
        duration_metadata.insert("stage".to_string(), stage_description.to_string());
        duration_metadata.insert("stage_type".to_string(), stage_type.to_string());
        duration_metadata.insert("record_subtype".to_string(), "duration".to_string());

        // Additional point for duration - can be used with Grafana Bar Gauge
        results.push(HealthRecord {
            record_type: "SleepDuration".to_string(),
            timestamp: start_timestamp,
            value: duration_minutes, // Duration in minutes for bar charts
            metadata: duration_metadata,
        });

        // Add a sleep state point for continuous state visualization
        let mut state_metadata = HashMap::new();
        state_metadata.insert("app_name".to_string(), app_name);
        state_metadata.insert("stage".to_string(), stage_description.to_string());
        state_metadata.insert("stage_type".to_string(), stage_type.to_string());

        // State point for Grafana State Timeline visualization
        results.push(HealthRecord {
            record_type: "SleepState".to_string(),
            timestamp: start_timestamp,
            value: stage_value, // Numeric value representing the sleep stage
            metadata: state_metadata,
        });

        Ok(results)
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

    /// Retrieves active calories data after a specific timestamp
    pub fn get_active_calories_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for active calories records
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT acb.start_time, acb.end_time, acb.energy, ai.app_name
                 FROM active_calories_burned_record_table acb
                 LEFT JOIN application_info_table ai ON acb.app_info_id = ai.row_id
                 WHERE acb.start_time > ? 
                 ORDER BY acb.start_time ASC"
                    .to_string()
            }
            None => "SELECT acb.start_time, acb.end_time, acb.energy, ai.app_name
                 FROM active_calories_burned_record_table acb
                 LEFT JOIN application_info_table ai ON acb.app_info_id = ai.row_id
                 ORDER BY acb.start_time ASC"
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
            match self.map_active_calories_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading active calories record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to an ActiveCalories HealthRecord
    fn map_active_calories_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let start_time_millis: i64 = row.get(0)?;
        let end_time_millis: i64 = row.get(1)?;
        let energy_value: f64 = row.get(2)?;
        let app_name: String = row.get(3).unwrap_or_else(|_| "unknown".to_string());

        let timestamp = Utc
            .timestamp_millis_opt(start_time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        // Calculate duration in minutes
        let duration_millis = end_time_millis - start_time_millis;
        let duration_minutes = duration_millis as f64 / (1000.0 * 60.0);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("unit".to_string(), "kcal".to_string());
        metadata.insert("duration_minutes".to_string(), duration_minutes.to_string());
        metadata.insert(
            "end_time".to_string(),
            Utc.timestamp_millis_opt(end_time_millis)
                .single()
                .unwrap_or_else(Utc::now)
                .to_rfc3339(),
        );

        Ok(HealthRecord {
            record_type: "ActiveCalories".to_string(),
            timestamp,
            value: energy_value,
            metadata,
        })
    }

    /// Retrieves total calories burned data after a specific timestamp
    pub fn get_total_calories_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for total calories records
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT tcb.start_time, tcb.end_time, tcb.energy, ai.app_name
                 FROM total_calories_burned_record_table tcb
                 LEFT JOIN application_info_table ai ON tcb.app_info_id = ai.row_id
                 WHERE tcb.start_time > ? 
                 ORDER BY tcb.start_time ASC"
                    .to_string()
            }
            None => "SELECT tcb.start_time, tcb.end_time, tcb.energy, ai.app_name
                 FROM total_calories_burned_record_table tcb
                 LEFT JOIN application_info_table ai ON tcb.app_info_id = ai.row_id
                 ORDER BY tcb.start_time ASC"
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
            match self.map_total_calories_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading total calories record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a TotalCalories HealthRecord
    fn map_total_calories_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let start_time_millis: i64 = row.get(0)?;
        let end_time_millis: i64 = row.get(1)?;
        let energy_value: f64 = row.get(2)?;
        let app_name: String = row.get(3).unwrap_or_else(|_| "unknown".to_string());

        let start_timestamp = Utc
            .timestamp_millis_opt(start_time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        // Calculate duration in hours for metadata
        let duration_millis = end_time_millis - start_time_millis;
        let duration_hours = duration_millis as f64 / (1000.0 * 60.0 * 60.0);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("unit".to_string(), "calories".to_string());
        metadata.insert("duration_hours".to_string(), duration_hours.to_string());
        metadata.insert(
            "start_time_millis".to_string(),
            start_time_millis.to_string(),
        );
        metadata.insert("end_time_millis".to_string(), end_time_millis.to_string());

        Ok(HealthRecord {
            record_type: "TotalCalories".to_string(),
            timestamp: start_timestamp,
            value: energy_value,
            metadata,
        })
    }

    /// Retrieves basal metabolic rate data after a specific timestamp
    pub fn get_basal_metabolic_rate_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for basal metabolic rate records
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT bmr.time, bmr.basal_metabolic_rate, ai.app_name
                 FROM basal_metabolic_rate_record_table bmr
                 LEFT JOIN application_info_table ai ON bmr.app_info_id = ai.row_id
                 WHERE bmr.time > ? 
                 ORDER BY bmr.time ASC"
                    .to_string()
            }
            None => "SELECT bmr.time, bmr.basal_metabolic_rate, ai.app_name
                 FROM basal_metabolic_rate_record_table bmr
                 LEFT JOIN application_info_table ai ON bmr.app_info_id = ai.row_id
                 ORDER BY bmr.time ASC"
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
            match self.map_basal_metabolic_rate_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading basal metabolic rate record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a BasalMetabolicRate HealthRecord
    fn map_basal_metabolic_rate_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let time_millis: i64 = row.get(0)?;
        let bmr_value: f64 = row.get(1)?;
        let app_name: String = row.get(2).unwrap_or_else(|_| "unknown".to_string());

        let timestamp = Utc
            .timestamp_millis_opt(time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("unit".to_string(), "calories_per_day".to_string());

        Ok(HealthRecord {
            record_type: "BasalMetabolicRate".to_string(),
            timestamp,
            value: bmr_value,
            metadata,
        })
    }

    /// Retrieves body fat percentage data after a specific timestamp
    pub fn get_body_fat_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for body fat records
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT bf.time, bf.percentage, ai.app_name
                 FROM body_fat_record_table bf
                 LEFT JOIN application_info_table ai ON bf.app_info_id = ai.row_id
                 WHERE bf.time > ? 
                 ORDER BY bf.time ASC"
                    .to_string()
            }
            None => "SELECT bf.time, bf.percentage, ai.app_name
                 FROM body_fat_record_table bf
                 LEFT JOIN application_info_table ai ON bf.app_info_id = ai.row_id
                 ORDER BY bf.time ASC"
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
            match self.map_body_fat_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading body fat record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to a BodyFat HealthRecord
    fn map_body_fat_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let time_millis: i64 = row.get(0)?;
        let percentage_value: f64 = row.get(1)?;
        let app_name: String = row.get(2).unwrap_or_else(|_| "unknown".to_string());

        let timestamp = Utc
            .timestamp_millis_opt(time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("unit".to_string(), "percentage".to_string());

        Ok(HealthRecord {
            record_type: "BodyFat".to_string(),
            timestamp,
            value: percentage_value,
            metadata,
        })
    }

    /// Retrieves exercise session data after a specific timestamp
    pub fn get_exercise_sessions_since(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Query for exercise session records
        let query = match since {
            Some(timestamp) => {
                let _unix_timestamp = timestamp.timestamp_millis();
                "SELECT es.start_time, es.end_time, es.exercise_type, es.title, ai.app_name
                 FROM exercise_session_record_table es
                 LEFT JOIN application_info_table ai ON es.app_info_id = ai.row_id
                 WHERE es.start_time > ? 
                 ORDER BY es.start_time ASC"
                    .to_string()
            }
            None => "SELECT es.start_time, es.end_time, es.exercise_type, es.title, ai.app_name
                 FROM exercise_session_record_table es
                 LEFT JOIN application_info_table ai ON es.app_info_id = ai.row_id
                 ORDER BY es.start_time ASC"
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
            match self.map_exercise_session_row(row_result) {
                Ok(record) => records.push(record),
                Err(e) => eprintln!("Error reading exercise session record: {}", e),
            }
        }

        Ok(records)
    }

    /// Maps a database row to an ExerciseSession HealthRecord
    fn map_exercise_session_row(&self, row: &Row) -> SqliteResult<HealthRecord> {
        let start_time_millis: i64 = row.get(0)?;
        let end_time_millis: i64 = row.get(1)?;
        let exercise_type: i64 = row.get(2)?;
        let title: String = row.get(3).unwrap_or_else(|_| "Unknown".to_string());
        let app_name: String = row.get(4).unwrap_or_else(|_| "unknown".to_string());

        let start_timestamp = Utc
            .timestamp_millis_opt(start_time_millis)
            .single()
            .unwrap_or_else(Utc::now);

        // Calculate duration in minutes
        let duration_millis = end_time_millis - start_time_millis;
        let duration_minutes = duration_millis as f64 / (1000.0 * 60.0);

        let mut metadata = HashMap::new();
        metadata.insert("app_name".to_string(), app_name);
        metadata.insert("exercise_type".to_string(), exercise_type.to_string());
        metadata.insert("title".to_string(), title);
        metadata.insert("duration_minutes".to_string(), duration_minutes.to_string());
        metadata.insert(
            "start_time_millis".to_string(),
            start_time_millis.to_string(),
        );
        metadata.insert("end_time_millis".to_string(), end_time_millis.to_string());
        metadata.insert("unit".to_string(), "minutes".to_string());

        Ok(HealthRecord {
            record_type: "ExerciseSession".to_string(),
            timestamp: start_timestamp,
            value: duration_minutes, // Use duration as the value for visualization
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

        // Get sleep data - this now includes multiple record types
        match self.get_sleep_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    // Split sleep records by record_type
                    let mut sleep_records = Vec::new();
                    let mut sleep_duration_records = Vec::new();
                    let mut sleep_state_records = Vec::new();

                    for record in records {
                        match record.record_type.as_str() {
                            "Sleep" => sleep_records.push(record),
                            "SleepDuration" => sleep_duration_records.push(record),
                            "SleepState" => sleep_state_records.push(record),
                            _ => sleep_records.push(record), // Default case
                        }
                    }

                    // Add each record type to the map
                    if !sleep_records.is_empty() {
                        all_data.insert("Sleep".to_string(), sleep_records);
                    }
                    if !sleep_duration_records.is_empty() {
                        all_data.insert("SleepDuration".to_string(), sleep_duration_records);
                    }
                    if !sleep_state_records.is_empty() {
                        all_data.insert("SleepState".to_string(), sleep_state_records);
                    }
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

        // Get active calories data
        match self.get_active_calories_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("ActiveCalories".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching active calories data: {}", e),
        }

        // Get total calories data
        match self.get_total_calories_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("TotalCalories".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching total calories data: {}", e),
        }

        // Get basal metabolic rate data
        match self.get_basal_metabolic_rate_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("BasalMetabolicRate".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching basal metabolic rate data: {}", e),
        }

        // Get body fat data
        match self.get_body_fat_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("BodyFat".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching body fat data: {}", e),
        }

        // Get exercise session data
        match self.get_exercise_sessions_since(since) {
            Ok(records) => {
                if !records.is_empty() {
                    all_data.insert("ExerciseSession".to_string(), records);
                }
            }
            Err(e) => eprintln!("Error fetching exercise session data: {}", e),
        }

        Ok(all_data)
    }

    /// Gets health data for specific data types since a specific timestamp
    /// data_types: List of data types to include (e.g., ["HeartRate", "Steps", "TotalCalories"])
    /// Available types: HeartRate, Steps, Sleep, SleepDuration, SleepState, Weight, ActiveCalories, TotalCalories, BasalMetabolicRate, BodyFat, ExerciseSession
    pub fn get_filtered_health_data_since(
        &self,
        since: Option<DateTime<Utc>>,
        data_types: &[String],
    ) -> Result<HashMap<String, Vec<HealthRecord>>, Box<dyn Error>> {
        let mut all_data = HashMap::new();

        // Helper function to check if a data type should be included
        let should_include = |data_type: &str| -> bool {
            data_types
                .iter()
                .any(|dt| dt.eq_ignore_ascii_case(data_type))
        };

        // Get heart rate data
        if should_include("HeartRate") {
            match self.get_heart_rate_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("HeartRate".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching heart rate data: {}", e),
            }
        }

        // Get steps data
        if should_include("Steps") {
            match self.get_steps_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("Steps".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching steps data: {}", e),
            }
        }

        // Get sleep data - this includes multiple record types
        if should_include("Sleep")
            || should_include("SleepDuration")
            || should_include("SleepState")
        {
            match self.get_sleep_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        // Split sleep records by record_type
                        let mut sleep_records = Vec::new();
                        let mut sleep_duration_records = Vec::new();
                        let mut sleep_state_records = Vec::new();

                        for record in records {
                            match record.record_type.as_str() {
                                "Sleep" => sleep_records.push(record),
                                "SleepDuration" => sleep_duration_records.push(record),
                                "SleepState" => sleep_state_records.push(record),
                                _ => sleep_records.push(record), // Default case
                            }
                        }

                        // Add each record type to the map based on what was requested
                        if should_include("Sleep") && !sleep_records.is_empty() {
                            all_data.insert("Sleep".to_string(), sleep_records);
                        }
                        if should_include("SleepDuration") && !sleep_duration_records.is_empty() {
                            all_data.insert("SleepDuration".to_string(), sleep_duration_records);
                        }
                        if should_include("SleepState") && !sleep_state_records.is_empty() {
                            all_data.insert("SleepState".to_string(), sleep_state_records);
                        }
                    }
                }
                Err(e) => eprintln!("Error fetching sleep data: {}", e),
            }
        }

        // Get weight data
        if should_include("Weight") {
            match self.get_weight_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("Weight".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching weight data: {}", e),
            }
        }

        // Get active calories data
        if should_include("ActiveCalories") {
            match self.get_active_calories_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("ActiveCalories".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching active calories data: {}", e),
            }
        }

        // Get total calories data
        if should_include("TotalCalories") {
            match self.get_total_calories_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("TotalCalories".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching total calories data: {}", e),
            }
        }

        // Get basal metabolic rate data
        if should_include("BasalMetabolicRate") {
            match self.get_basal_metabolic_rate_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("BasalMetabolicRate".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching basal metabolic rate data: {}", e),
            }
        }

        // Get body fat data
        if should_include("BodyFat") {
            match self.get_body_fat_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("BodyFat".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching body fat data: {}", e),
            }
        }

        // Get exercise session data
        if should_include("ExerciseSession") {
            match self.get_exercise_sessions_since(since) {
                Ok(records) => {
                    if !records.is_empty() {
                        all_data.insert("ExerciseSession".to_string(), records);
                    }
                }
                Err(e) => eprintln!("Error fetching exercise session data: {}", e),
            }
        }

        Ok(all_data)
    }

    /// Retrieves heart rate data with gap-filling for the last week
    /// This method checks what data already exists in InfluxDB and only imports missing data points
    pub async fn get_heart_rate_with_gap_filling(
        &self,
        influx_client: &crate::influx_client::InfluxClient,
        days_back: i64,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        println!(
            "Starting heart rate gap-filling for the last {} days",
            days_back
        );

        // Get existing timestamps from InfluxDB
        let existing_timestamps = influx_client
            .get_existing_heart_rate_timestamps(days_back)
            .await?;

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        // Calculate the time range for the last week
        let end_time = Utc::now();
        let start_time = end_time - chrono::Duration::days(days_back);
        let start_timestamp_millis = start_time.timestamp_millis();

        println!();
        println!("📊 Heart Rate Gap-Filling Analysis");
        println!("=====================================");
        println!(
            "Time range: {} to {} ({} days)",
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S"),
            days_back
        );
        println!(
            "InfluxDB existing data points: {}",
            existing_timestamps.len()
        );

        // First, count total records in the time range to show progress
        let count_query = "SELECT COUNT(*) FROM heart_rate_record_series_table hrs
                          WHERE hrs.epoch_millis >= ?";

        let total_db_records = match conn.prepare(count_query) {
            Ok(mut stmt) => {
                match stmt.query_row([start_timestamp_millis], |row| row.get::<_, i64>(0)) {
                    Ok(count) => count,
                    Err(_) => 0,
                }
            }
            Err(_) => 0,
        };

        println!(
            "SQLite database records (time range):   {}",
            total_db_records
        );
        println!();

        if total_db_records == 0 {
            println!(
                "⚠️  No heart rate data found in SQLite database for the specified time range"
            );
            return Ok(Vec::new());
        }

        println!("🔍 Processing records and checking for gaps...");

        // Query for heart rate records from the last week
        let query = "SELECT hrs.epoch_millis, hrs.beats_per_minute, ai.app_name
                     FROM heart_rate_record_series_table hrs
                     LEFT JOIN heart_rate_record_table hrr ON hrs.parent_key = hrr.row_id
                     LEFT JOIN application_info_table ai ON hrr.app_info_id = ai.row_id
                     WHERE hrs.epoch_millis >= ?
                     ORDER BY hrs.epoch_millis ASC";

        let mut stmt = match conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                // If the table doesn't exist, return empty results
                if e.to_string().contains("no such table") {
                    println!("Heart rate table not found in database");
                    return Ok(Vec::new());
                }
                return Err(Box::new(e));
            }
        };

        let mut rows = stmt.query([start_timestamp_millis])?;
        let mut total_count = 0;
        let mut new_count = 0;
        let mut duplicate_count = 0;
        let progress_interval = std::cmp::max(1, total_db_records / 10); // Show progress every 10%

        while let Some(row_result) = rows.next()? {
            total_count += 1;

            // Show progress every 10% or for smaller datasets, every 1000 records
            if total_count % progress_interval == 0 || total_count % 1000 == 0 {
                let progress_percent = (total_count as f64 / total_db_records as f64) * 100.0;
                println!(
                    "  Progress: {:.1}% ({}/{} records processed, {} gaps found so far)",
                    progress_percent, total_count, total_db_records, new_count
                );
            }

            // Get the timestamp from the row to check if it already exists
            let time_millis: i64 = row_result.get(0)?;

            // Check if this timestamp already exists in InfluxDB
            if existing_timestamps.contains(&time_millis) {
                duplicate_count += 1;
                continue; // Skip this record as it already exists
            }

            // This is a new record, add it to the import list
            match self.map_heart_rate_row(row_result) {
                Ok(record) => {
                    records.push(record);
                    new_count += 1;
                }
                Err(e) => eprintln!("Error reading heart rate record: {}", e),
            }
        }

        println!();
        println!("📈 Gap-Filling Summary");
        println!("======================");
        println!(
            "SQLite database records (last {} days): {}",
            days_back, total_count
        );
        println!(
            "InfluxDB existing records:               {}",
            duplicate_count
        );
        println!("Gap-filled records to import:            {}", new_count);
        println!();

        if total_count > 0 {
            let coverage_percent = (duplicate_count as f64 / total_count as f64) * 100.0;
            println!(
                "📊 Data Coverage: {:.1}% ({} of {} records already in InfluxDB)",
                coverage_percent, duplicate_count, total_count
            );

            if new_count > 0 {
                println!(
                    "🔄 Action: {} new records will be imported to fill gaps",
                    new_count
                );
            } else {
                println!("✅ Action: No gaps found - all data is already in InfluxDB");
            }
        } else {
            println!(
                "⚠️  No heart rate data found in SQLite database for the specified time range"
            );
        }

        Ok(records)
    }
}
