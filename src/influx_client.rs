use crate::csv_parser::CsvRecord;
use crate::health_data::HealthRecord;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use influxdb::{Client, InfluxDbWriteable, ReadQuery, Timestamp};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::error::Error;

/// Represents a client for connecting to InfluxDB
pub struct InfluxClient {
    client: Client,
    // org: String,
    // bucket: String,
    dry_run: bool,
}

/// Represents a data point to be written to InfluxDB
#[derive(Serialize, Clone, Debug)]
pub struct DataPoint {
    /// The measurement name in InfluxDB
    pub measurement: String,
    /// The timestamp for the data point
    pub time: DateTime<Utc>,
    /// The tag set for the data point
    pub tags: HashMap<String, String>,
    /// The field set for the data point
    pub field_value: f64,
}

impl InfluxClient {
    /// Creates a new InfluxDB client
    pub fn new(url: &str, bucket: &str, token: &str) -> Self {
        let client = Client::new(url, bucket).with_token(token);

        InfluxClient {
            client,
            // org: org.to_string(),
            // bucket: bucket.to_string(),
            dry_run: false,
        }
    }

    /// Creates a new InfluxDB client in dry-run mode
    pub fn new_dry_run(url: &str, bucket: &str, token: &str) -> Self {
        let client = Client::new(url, bucket).with_token(token);

        InfluxClient {
            client,
            // org: org.to_string(),
            // bucket: bucket.to_string(),
            dry_run: true,
        }
    }

    /// Converts a CSV record to multiple InfluxDB data points
    /// Each column (except the timestamp column) becomes a separate measurement
    /// To be used for funds records
    pub fn convert_funds_record(
        &self,
        record: &CsvRecord,
        time_column: &str,
        time_format: &str,
    ) -> Result<Vec<DataPoint>, Box<dyn Error>> {
        assert!(
            record.header_values.len() == 2,
            "There should be two header rows"
        );

        let mut data_points = Vec::new();

        // Get the timestamp value from the specified column
        let time_column_index = match record.column_indexes.get(time_column) {
            Some(idx) => *idx,
            None => return Err(format!("Time column '{}' not found", time_column).into()),
        };

        // Ensure the time column index is valid
        if time_column_index >= record.values.len() {
            return Err(format!("Time column index {} out of bounds", time_column_index).into());
        }

        // Parse the timestamp value
        let time_value = &record.values[time_column_index];
        let naive_dt = match NaiveDateTime::parse_from_str(time_value, time_format) {
            Ok(dt) => dt,
            Err(e) => {
                return Err(format!("Failed to parse timestamp '{}': {}", time_value, e).into())
            }
        };
        let timestamp = DateTime::from_naive_utc_and_offset(naive_dt, Utc);

        // Process each column (except timestamp) as a separate measurement
        for (col_name, col_idx) in &record.column_indexes {
            // Skip the timestamp column
            if col_name == time_column {
                continue;
            }

            // Skip columns with invalid indices
            if *col_idx >= record.values.len() {
                continue;
            }

            let mut value = record.values[*col_idx].clone();

            // Try to convert column value to float

            // first let's check if the value is a currency
            if value.contains('$') || value.contains('€') {
                // Remove the currency symbol and any commas
                value = value.replace(['$', '€', ','], "").trim().to_string();
            }

            // then let's check if the value is a percentage
            if value.ends_with('%') {
                // Remove the percentage symbol
                value = value.trim_end_matches('%').to_string();
            }

            match value.parse::<f64>() {
                Ok(float_value) => {
                    // This column contains a numeric value - create a data point
                    let mut tags = HashMap::new();

                    // Extract tags from header rows for this column
                    // Safely access the first header row and check if column index is valid
                    if !record.header_values.is_empty() && *col_idx < record.header_values[0].len()
                    {
                        let header_value = &record.header_values[0][*col_idx]
                            .replace(['\n', '\r'], " ")
                            .replace(' ', "_")
                            .replace("__", "_");

                        if !header_value.is_empty() {
                            tags.insert("fondo".to_string(), header_value.clone());
                        }
                    }

                    // Extract measurement from the second header row
                    // Safely access the last header row and check if column index is valid
                    let measurement = if record.header_values.len() > 1
                        && *col_idx < record.header_values[1].len()
                    {
                        &record.header_values[1][*col_idx]
                    } else {
                        // Use column name as fallback if header information is not available
                        col_name.split('.').next_back().unwrap_or(col_name)
                    };

                    // Create the data point
                    data_points.push(DataPoint {
                        measurement: measurement.to_string(),
                        time: timestamp,
                        tags,
                        field_value: float_value,
                    });
                }
                Err(_) => {
                    // Non-numeric values could be skipped or handled differently
                    // For now, we'll just skip them
                    continue;
                }
            }
        }

        if data_points.is_empty() {
            return Err("No valid measurements found in record".into());
        }

        Ok(data_points)
    }

    #[allow(dead_code)]
    /// Writes a data point to InfluxDB
    pub async fn write_point(&self, point: DataPoint) -> Result<String, Box<dyn Error>> {
        // Create a write query for the data point
        let mut write_query = Timestamp::from(point.time)
            .into_query(point.measurement)
            .add_field("value", point.field_value);
        for (tag_name, tag_value) in point.tags {
            write_query = write_query.add_tag(tag_name, tag_value);
        }

        if self.dry_run {
            println!("Dry-run mode: Would write point: {:?}", write_query);
            return Ok("Dry-run mode: Point not written".to_string());
        }

        self.client.query(write_query).await.map_err(|e| e.into())
    }

    /// Writes multiple data points to InfluxDB in a single request
    pub async fn write_points(&self, points: &[DataPoint]) -> Result<(), Box<dyn Error>> {
        if points.is_empty() {
            return Ok(());
        }

        if self.dry_run {
            println!(
                "Dry-run mode: Would write {} points to InfluxDB",
                points.len()
            );
            for (i, point) in points.iter().enumerate() {
                // Limit the number of points to display in dry-run mode
                if i >= 10 && points.len() > 20 {
                    println!("... and {} more points (not shown)", points.len() - 10);
                    break;
                }

                // Create a write query for the data point to display
                let mut write_query = Timestamp::from(point.time)
                    .into_query(&point.measurement)
                    .add_field("value", point.field_value);
                for (tag_name, tag_value) in point.tags.clone() {
                    write_query = write_query.add_tag(tag_name, tag_value);
                }

                println!("[{}/{}] Query: {:?}", i + 1, points.len(), write_query);
            }
            return Ok(());
        }

        // Batch size - balance between performance and memory usage
        // InfluxDB typically handles batches of up to 5000 points efficiently
        const BATCH_SIZE: usize = 1000;

        // Process points in batches to improve performance
        for chunk in points.chunks(BATCH_SIZE) {
            // Create a vector of write queries for this batch
            let mut batch_queries = Vec::with_capacity(chunk.len());

            for point in chunk {
                // Create a write query for the data point
                let mut write_query = Timestamp::from(point.time)
                    .into_query(&point.measurement)
                    .add_field("value", point.field_value);

                // Add all tags to the query
                for (tag_name, tag_value) in &point.tags {
                    write_query = write_query.add_tag(tag_name, tag_value.clone());
                }

                batch_queries.push(write_query);
            }

            // Execute the batch write - the Vec<WriteQuery> is automatically handled by the client
            match self.client.query(batch_queries).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error writing batch to InfluxDB: {}", e);
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    /// Process and write all CSV records to InfluxDB
    pub async fn write_funds_records(
        &self,
        records: &[CsvRecord],
        time_column: &str,
        time_format: &str,
    ) -> Result<usize, Box<dyn Error>> {
        let mut all_points = Vec::new();
        let mut error_count = 0;
        let mut success_count = 0;

        for record in records {
            match self.convert_funds_record(record, time_column, time_format) {
                Ok(points) => {
                    success_count += points.len();
                    all_points.extend(points);
                }
                Err(e) => {
                    eprintln!("Error converting record: {}", e);
                    error_count += 1;
                }
            }
        }

        if self.dry_run {
            println!(
                "Dry-run mode: Would write {} data points to InfluxDB",
                all_points.len()
            );
        } else {
            println!("Writing {} data points to InfluxDB", all_points.len());
        }

        self.write_points(&all_points).await?;

        if error_count > 0 {
            eprintln!("Failed to convert {} records", error_count);
        }

        Ok(success_count)
    }

    /// Process and write all health records to InfluxDB
    pub async fn write_health_records(
        &self,
        records_map: &HashMap<String, Vec<HealthRecord>>,
    ) -> Result<usize, Box<dyn Error>> {
        let mut all_points = Vec::new();
        let mut success_count = 0;

        for (record_type, records) in records_map {
            println!("Processing {} {} records", records.len(), record_type);

            for record in records {
                // Convert health record to InfluxDB data point
                let mut tags = HashMap::new();

                // Add any metadata as tags
                for (key, value) in &record.metadata {
                    tags.insert(key.clone(), value.clone());
                }

                // Add record type as a tag for easier querying
                tags.insert("record_type".to_string(), record_type.clone());

                // Create data point
                let point = DataPoint {
                    measurement: record_type.clone(),
                    time: record.timestamp,
                    tags,
                    field_value: record.value,
                };

                all_points.push(point);
                success_count += 1;
            }
        }

        if self.dry_run {
            println!(
                "Dry-run mode: Would write {} health data points to InfluxDB",
                all_points.len()
            );
        } else {
            println!(
                "Writing {} health data points to InfluxDB",
                all_points.len()
            );
        }

        self.write_points(&all_points).await?;

        Ok(success_count)
    }

    /// Queries existing heart rate data from InfluxDB for the last week
    /// Returns a set of timestamps (as Unix milliseconds) that already exist
    pub async fn get_existing_heart_rate_timestamps(
        &self,
        days_back: i64,
    ) -> Result<HashSet<i64>, Box<dyn Error>> {
        let end_time = Utc::now();
        let start_time = end_time - Duration::days(days_back);

        // Convert to Unix timestamps in milliseconds
        let start_timestamp = start_time.timestamp_millis();
        let end_timestamp = end_time.timestamp_millis();

        // InfluxQL query to get existing heart rate timestamps
        let query = format!(
            "SELECT time, value FROM \"HeartRate\" WHERE time >= {}ms AND time <= {}ms",
            start_timestamp, end_timestamp
        );

        println!(
            "Querying existing heart rate data from {} to {} ({} days)",
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S"),
            days_back
        );

        if self.dry_run {
            println!("  (Dry-run mode: Querying InfluxDB for existing data, but won't write new data)");
        }

        let mut existing_timestamps = HashSet::new();

        match self.client.json_query(ReadQuery::new(query)).await {
            Ok(read_result) => {
                // Check if there are results
                for result in &read_result.results {
                    if let Some(series_value) = result.get("series") {
                        if let Some(series_array) = series_value.as_array() {
                            for serie_value in series_array {
                                if let Some(values_value) = serie_value.get("values") {
                                    if let Some(values_array) = values_value.as_array() {
                                        for value_row in values_array {
                                            if let Some(value_array) = value_row.as_array() {
                                                if let Some(timestamp_value) = value_array.get(0) {
                                                    if let Some(timestamp_str) =
                                                        timestamp_value.as_str()
                                                    {
                                                        // InfluxDB returns timestamps in RFC3339 format
                                                        if let Ok(parsed_time) =
                                                            DateTime::parse_from_rfc3339(
                                                                timestamp_str,
                                                            )
                                                        {
                                                            let timestamp_millis =
                                                                parsed_time.timestamp_millis();
                                                            existing_timestamps
                                                                .insert(timestamp_millis);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                println!(
                    "Found {} existing heart rate data points in InfluxDB",
                    existing_timestamps.len()
                );
            }
            Err(e) => {
                println!("Warning: Failed to query existing heart rate data: {}", e);
                println!("Proceeding with normal import (may result in duplicates)");
            }
        }

        Ok(existing_timestamps)
    }
}
