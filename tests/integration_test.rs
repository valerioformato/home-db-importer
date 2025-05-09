use chrono::{DateTime, NaiveDateTime, Utc};
use home_db_importer::influx_client::{DataPoint, InfluxClient};
use std::collections::HashMap;

// Helper function to create test DataPoints
fn create_test_point(measurement: &str, value: f64, timestamp: &str) -> DataPoint {
    let naive_dt = NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").unwrap();
    let dt = DateTime::from_naive_utc_and_offset(naive_dt, Utc);

    let mut tags = HashMap::new();
    tags.insert("test_tag".to_string(), "test_value".to_string());

    DataPoint {
        measurement: measurement.to_string(),
        time: dt,
        tags,
        field_value: value,
    }
}

#[tokio::test]
async fn test_dry_run_write_point() {
    // Create a client in dry-run mode
    let client = InfluxClient::new_dry_run("http://localhost:8086", "bucket", "token");

    // Create a sample data point
    let data_point = create_test_point("test_measurement", 42.0, "2023-01-15 10:00:00");

    // In dry-run mode, write_point should return a success result containing "Dry-run mode"
    let result = client.write_point(data_point).await;
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Dry-run mode"));
}

#[tokio::test]
async fn test_dry_run_write_points() {
    // Create a client in dry-run mode
    let client = InfluxClient::new_dry_run("http://localhost:8086", "bucket", "token");

    // Create sample data points
    let points = vec![
        create_test_point("test1", 42.0, "2023-01-15 10:00:00"),
        create_test_point("test2", 43.0, "2023-01-15 10:01:00"),
        create_test_point("test3", 44.0, "2023-01-15 10:02:00"),
    ];

    // In dry-run mode, write_points should return success without sending data
    let result = client.write_points(&points).await;
    assert!(result.is_ok());
}
