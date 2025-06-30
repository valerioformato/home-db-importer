// Debug tool to check what's actually in InfluxDB
use chrono::{DateTime, Duration, Utc};
use influxdb::{Client, ReadQuery};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Your actual InfluxDB credentials
    let url = "http://192.168.2.176:30115";
    let bucket = "health";
    let token =
        "IrWxsqX4pGImBXp1bMhE7FMIhGHvEhn_5lb798yTVMOvAxNks67vV5XaRWLEX4Vau3dAC9B1U-TgDliTPaEzmA==";

    let client = Client::new(url, bucket).with_token(token);

    println!("=== InfluxDB Diagnostic Tool ===");
    println!("URL: {}", url);
    println!("Bucket: {}", bucket);
    println!();

    // Query for recent heart rate data (last 7 days only)
    let end_time = Utc::now();
    let start_time = end_time - Duration::days(7);
    let start_timestamp = start_time.timestamp_millis();
    let end_timestamp = end_time.timestamp_millis();

    let query = format!(
        "SELECT time, value FROM \"HeartRate\" WHERE time >= {}ms AND time <= {}ms ORDER BY time DESC LIMIT 10",
        start_timestamp, end_timestamp
    );
    println!("Querying InfluxDB for heart rate data from last 7 days...");
    println!(
        "Time range: {} to {}",
        start_time.format("%Y-%m-%d %H:%M:%S"),
        end_time.format("%Y-%m-%d %H:%M:%S")
    );
    println!("Query: {}", query);
    println!();

    match client.json_query(ReadQuery::new(query)).await {
        Ok(read_result) => {
            let mut record_count = 0;
            println!("Recent HeartRate data (last 7 days):");

            for result in &read_result.results {
                if let Some(series_value) = result.get("series") {
                    if let Some(series_array) = series_value.as_array() {
                        for serie_value in series_array {
                            if let Some(values_value) = serie_value.get("values") {
                                if let Some(values_array) = values_value.as_array() {
                                    for value_row in values_array {
                                        if let Some(value_array) = value_row.as_array() {
                                            if let Some(timestamp_value) = value_array.get(0) {
                                                if let Some(heart_rate_value) = value_array.get(1) {
                                                    record_count += 1;
                                                    println!(
                                                        "  {}. Time: {}, HeartRate: {}",
                                                        record_count,
                                                        timestamp_value,
                                                        heart_rate_value
                                                    );
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

            if record_count == 0 {
                println!("  No heart rate data found in the last 7 days");
            } else {
                println!(
                    "\nFound {} heart rate records in the last 7 days",
                    record_count
                );
            }
        }
        Err(e) => {
            println!("Error querying InfluxDB: {}", e);
        }
    }

    // Also check total count for comparison
    println!("\n=== Total HeartRate data count ===");
    let count_query = "SELECT COUNT(value) FROM \"HeartRate\"";
    println!("Query: {}", count_query);

    match client.json_query(ReadQuery::new(count_query)).await {
        Ok(read_result) => {
            for result in &read_result.results {
                if let Some(series_value) = result.get("series") {
                    if let Some(series_array) = series_value.as_array() {
                        for serie_value in series_array {
                            if let Some(values_value) = serie_value.get("values") {
                                if let Some(values_array) = values_value.as_array() {
                                    for value_row in values_array {
                                        if let Some(value_array) = value_row.as_array() {
                                            if let Some(count_value) = value_array.get(1) {
                                                println!(
                                                    "Total HeartRate records: {}",
                                                    count_value
                                                );
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
        Err(e) => {
            println!("Error counting records: {}", e);
        }
    }

    Ok(())
}
