use crate::csv_parser::CsvParser;
use crate::influx_client::{DataPoint, InfluxClient};
use chrono::NaiveDateTime;
use std::collections::HashMap;
use std::error::Error;

/// This example demonstrates how to parse a time-series CSV and convert it to InfluxDB data points
/// The CSV has the first column as a timestamp and each other column represents a separate measurement
pub async fn process_time_series_csv(
    csv_path: &str,
    influx_url: &str,
    influx_org: &str,
    influx_bucket: &str,
    influx_token: &str,
) -> Result<(), Box<dyn Error>> {
    // Create the CSV parser
    let parser = CsvParser::new(csv_path)
        .with_header_rows(2) // First two rows contain headers with measurement metadata
        .with_time_column_index(Some(0)); // First column is timestamp

    // Parse the CSV
    let records = parser.parse()?;
    println!("Parsed {} records from CSV", records.len());

    // Create the InfluxDB client
    let client = InfluxClient::new(influx_url, influx_org, influx_bucket, influx_token);

    // Process and write all records with "2023-01-01 00:00:00" format for timestamps
    let written = client
        .write_records(&records, "sensor_", "Date", "%Y-%m-%d %H:%M:%S")
        .await?;

    println!("Successfully wrote {} measurements to InfluxDB", written);
    Ok(())
}

/// This example demonstrates manual conversion from CSV to InfluxDB data points
/// It shows how to interpret the header rows as tag values
pub fn manual_csv_to_datapoints(csv_path: &str) -> Result<Vec<DataPoint>, Box<dyn Error>> {
    // Create and parse the CSV
    let parser = CsvParser::new(csv_path)
        .with_header_rows(2)  // First two rows contain headers with measurement metadata
        .with_time_column_index(Some(0)); // First column is timestamp
    
    let records = parser.parse()?;
    
    let mut data_points = Vec::new();
    
    // Iterate through each record
    for record in &records {
        // Get timestamp from first column
        if let Some(time_str) = record.get_time_value() {
            // Parse the timestamp 
            let naive_dt = NaiveDateTime::parse_from_str(time_str, "%Y-%m-%d %H:%M:%S")?;
            let timestamp = chrono::DateTime::from_naive_utc_and_offset(naive_dt, chrono::Utc);
            
            // For each column that's not the timestamp column, create a separate measurement
            for column_name in record.get_measurement_columns() {
                if let Some(value_str) = record.get_measurement_value(column_name) {
                    // Try to parse the value as a float
                    if let Ok(value) = value_str.parse::<f64>() {
                        // Extract tag values from header rows for this column
                        let mut tags = HashMap::new();
                        
                        // Get column index for the current column
                        if let Some(col_idx) = record.column_indexes.get(column_name) {
                            // Extract tag values from each header row for this column
                            for (row_idx, header_row) in record.header_values.iter().enumerate() {
                                if *col_idx < header_row.len() {
                                    let header_value = &header_row[*col_idx];
                                    if !header_value.is_empty() {
                                        // First header row might contain category/sensor type
                                        if row_idx == 0 {
                                            tags.insert("sensor_type".to_string(), header_value.clone());
                                        }
                                        // Second header row might contain unit/location
                                        else if row_idx == 1 {
                                            tags.insert("location".to_string(), header_value.clone());
                                        }
                                        // Generic fallback for any other header rows
                                        else {
                                            tags.insert(format!("header_{}", row_idx), header_value.clone());
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Create fields map with the single value
                        let mut fields = HashMap::new();
                        fields.insert("value".to_string(), value);
                        
                        // Add a data point for this measurement
                        data_points.push(DataPoint {
                            measurement: format!("sensor_{}", column_name),
                            timestamp: Some(timestamp),
                            tags,
                            fields,
                        });
                    }
                }
            }
        }
    }
    
    Ok(data_points)
}

/// Example of how header cells might represent tag values in a home monitoring CSV:
/// 
/// ```text
/// Date,                Temperature, Temperature, Humidity, Power
/// (timestamp),         Indoor,      Outdoor,     Indoor,   Main
/// 2023-01-01 00:00:00, 21.5,        10.2,        45.3,     1200.5
/// 2023-01-01 01:00:00, 21.3,        9.8,         46.1,     980.2
/// ```
/// 
/// In this example:
/// - First column = timestamp
/// - Header row 1 = measurement type (Temperature, Humidity, Power)
/// - Header row 2 = location/sensor (Indoor, Outdoor, Main)
///
/// Each data point would have tags like:
/// - sensor_type: "Temperature"
/// - location: "Indoor"
///
/// The resulting InfluxDB structure would have measurements:
/// - sensor_Temperature with tags {sensor_type: "Temperature", location: "Indoor"}
/// - sensor_Temperature with tags {sensor_type: "Temperature", location: "Outdoor"}
/// - sensor_Humidity with tags {sensor_type: "Humidity", location: "Indoor"}
/// - sensor_Power with tags {sensor_type: "Power", location: "Main"}