// Simple example to test the new health data types
// Since this is an external example, we'll import the necessary modules
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{Connection, Result as SqliteResult, Row};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

// Copy the necessary structs and implementations for this example
#[derive(Debug, Clone)]
pub struct HealthRecord {
    pub record_type: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub metadata: HashMap<String, String>,
}

pub struct HealthDataReader {
    db_path: String,
}

impl HealthDataReader {
    pub fn new(db_path: &str) -> Self {
        HealthDataReader {
            db_path: db_path.to_string(),
        }
    }

    pub fn db_exists(&self) -> bool {
        Path::new(&self.db_path).exists()
    }

    pub fn open_connection(&self) -> SqliteResult<Connection> {
        Connection::open(&self.db_path)
    }

    pub fn get_basal_metabolic_rate_since(
        &self,
        _since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        let query = "SELECT bmr.time, bmr.basal_metabolic_rate, ai.app_name
                     FROM basal_metabolic_rate_record_table bmr
                     LEFT JOIN application_info_table ai ON bmr.app_info_id = ai.row_id
                     ORDER BY bmr.time ASC LIMIT 5";

        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;

        while let Some(row_result) = rows.next()? {
            let time_millis: i64 = row_result.get(0)?;
            let bmr_value: f64 = row_result.get(1)?;
            let app_name: String = row_result.get(2).unwrap_or_else(|_| "unknown".to_string());

            let timestamp = Utc
                .timestamp_millis_opt(time_millis)
                .single()
                .unwrap_or_else(Utc::now);

            let mut metadata = HashMap::new();
            metadata.insert("app_name".to_string(), app_name);
            metadata.insert("unit".to_string(), "calories_per_day".to_string());

            records.push(HealthRecord {
                record_type: "BasalMetabolicRate".to_string(),
                timestamp,
                value: bmr_value,
                metadata,
            });
        }

        Ok(records)
    }

    pub fn get_body_fat_since(
        &self,
        _since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        let query = "SELECT bf.time, bf.percentage, ai.app_name
                     FROM body_fat_record_table bf
                     LEFT JOIN application_info_table ai ON bf.app_info_id = ai.row_id
                     ORDER BY bf.time ASC LIMIT 5";

        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;

        while let Some(row_result) = rows.next()? {
            let time_millis: i64 = row_result.get(0)?;
            let percentage_value: f64 = row_result.get(1)?;
            let app_name: String = row_result.get(2).unwrap_or_else(|_| "unknown".to_string());

            let timestamp = Utc
                .timestamp_millis_opt(time_millis)
                .single()
                .unwrap_or_else(Utc::now);

            let mut metadata = HashMap::new();
            metadata.insert("app_name".to_string(), app_name);
            metadata.insert("unit".to_string(), "percentage".to_string());

            records.push(HealthRecord {
                record_type: "BodyFat".to_string(),
                timestamp,
                value: percentage_value,
                metadata,
            });
        }

        Ok(records)
    }

    pub fn get_exercise_sessions_since(
        &self,
        _since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HealthRecord>, Box<dyn Error>> {
        if !self.db_exists() {
            return Err(format!("Database file does not exist: {}", self.db_path).into());
        }

        let conn = self.open_connection()?;
        let mut records = Vec::new();

        let query = "SELECT es.start_time, es.end_time, es.exercise_type, es.title, ai.app_name
                     FROM exercise_session_record_table es
                     LEFT JOIN application_info_table ai ON es.app_info_id = ai.row_id
                     ORDER BY es.start_time ASC LIMIT 5";

        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;

        while let Some(row_result) = rows.next()? {
            let start_time_millis: i64 = row_result.get(0)?;
            let end_time_millis: i64 = row_result.get(1)?;
            let exercise_type: i64 = row_result.get(2)?;
            let title: String = row_result.get(3).unwrap_or_else(|_| "Unknown".to_string());
            let app_name: String = row_result.get(4).unwrap_or_else(|_| "unknown".to_string());

            let start_timestamp = Utc
                .timestamp_millis_opt(start_time_millis)
                .single()
                .unwrap_or_else(Utc::now);

            let duration_millis = end_time_millis - start_time_millis;
            let duration_minutes = duration_millis as f64 / (1000.0 * 60.0);

            let mut metadata = HashMap::new();
            metadata.insert("app_name".to_string(), app_name);
            metadata.insert("exercise_type".to_string(), exercise_type.to_string());
            metadata.insert("title".to_string(), title);
            metadata.insert("duration_minutes".to_string(), duration_minutes.to_string());
            metadata.insert("unit".to_string(), "minutes".to_string());

            records.push(HealthRecord {
                record_type: "ExerciseSession".to_string(),
                timestamp: start_timestamp,
                value: duration_minutes,
                metadata,
            });
        }

        Ok(records)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = HealthDataReader::new("tests/health_connect_export.db");

    println!("Testing new health data types...\n");

    // Test Basal Metabolic Rate
    println!("=== Basal Metabolic Rate ===");
    match reader.get_basal_metabolic_rate_since(None) {
        Ok(records) => {
            println!("Found {} BMR records", records.len());
            if let Some(first_record) = records.first() {
                println!("Sample record:");
                println!("  Type: {}", first_record.record_type);
                println!("  Timestamp: {}", first_record.timestamp);
                println!("  Value: {} calories/day", first_record.value);
                println!("  Metadata: {:?}", first_record.metadata);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Test Body Fat
    println!("\n=== Body Fat ===");
    match reader.get_body_fat_since(None) {
        Ok(records) => {
            println!("Found {} body fat records", records.len());
            if let Some(first_record) = records.first() {
                println!("Sample record:");
                println!("  Type: {}", first_record.record_type);
                println!("  Timestamp: {}", first_record.timestamp);
                println!("  Value: {}%", first_record.value);
                println!("  Metadata: {:?}", first_record.metadata);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Test Exercise Sessions
    println!("\n=== Exercise Sessions ===");
    match reader.get_exercise_sessions_since(None) {
        Ok(records) => {
            println!("Found {} exercise session records", records.len());
            if let Some(first_record) = records.first() {
                println!("Sample record:");
                println!("  Type: {}", first_record.record_type);
                println!("  Timestamp: {}", first_record.timestamp);
                println!("  Value: {} minutes", first_record.value);
                println!("  Metadata: {:?}", first_record.metadata);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    Ok(())
}
