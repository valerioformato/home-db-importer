// Simple tool to debug InfluxDB connection and query issues
use influxdb::{Client, ReadQuery};
use chrono::{DateTime, Utc, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "http://192.168.2.176:30115";
    let bucket = "health";
    let token = "IrWxsqX4pGImBXp1bMhE7FMIhGHvEhn_5lb798yTVMOvAxNks67vV5XaRWLEX4Vau3dAC9B1U-TgDliTPaEzmA==";
    
    let client = Client::new(url, bucket).with_token(token);
    
    println!("Testing InfluxDB connection to: {}", url);
    println!("Bucket: {}", bucket);
    
    // Test 1: Simple query to see what measurements exist
    println!("\n=== Test 1: Show measurements ===");
    let show_measurements_query = "SHOW MEASUREMENTS";
    match client.json_query(ReadQuery::new(show_measurements_query)).await {
        Ok(result) => {
            println!("Measurements query successful!");
            println!("Result: {:#?}", result);
        }
        Err(e) => {
            println!("Failed to query measurements: {}", e);
        }
    }
    
    // Test 2: Count all HeartRate records
    println!("\n=== Test 2: Count HeartRate records ===");
    let count_query = "SELECT COUNT(*) FROM \"HeartRate\"";
    match client.json_query(ReadQuery::new(count_query)).await {
        Ok(result) => {
            println!("Count query successful!");
            println!("Result: {:#?}", result);
        }
        Err(e) => {
            println!("Failed to count HeartRate records: {}", e);
        }
    }
    
    // Test 3: Get recent HeartRate records (last 24 hours)
    println!("\n=== Test 3: Recent HeartRate records (last 24 hours) ===");
    let end_time = Utc::now();
    let start_time = end_time - Duration::hours(24);
    let start_timestamp = start_time.timestamp_millis();
    let end_timestamp = end_time.timestamp_millis();
    
    let recent_query = format!(
        "SELECT time, value FROM \"HeartRate\" WHERE time >= {}ms AND time <= {}ms LIMIT 10",
        start_timestamp, end_timestamp
    );
    println!("Query: {}", recent_query);
    
    match client.json_query(ReadQuery::new(recent_query)).await {
        Ok(result) => {
            println!("Recent records query successful!");
            println!("Result: {:#?}", result);
        }
        Err(e) => {
            println!("Failed to query recent records: {}", e);
        }
    }
    
    // Test 4: Get any HeartRate records (last 30 days)
    println!("\n=== Test 4: Any HeartRate records (last 30 days) ===");
    let start_time_30d = end_time - Duration::days(30);
    let start_timestamp_30d = start_time_30d.timestamp_millis();
    
    let any_query = format!(
        "SELECT time, value FROM \"HeartRate\" WHERE time >= {}ms LIMIT 10",
        start_timestamp_30d
    );
    println!("Query: {}", any_query);
    
    match client.json_query(ReadQuery::new(any_query)).await {
        Ok(result) => {
            println!("Any records query successful!");
            println!("Result: {:#?}", result);
        }
        Err(e) => {
            println!("Failed to query any records: {}", e);
        }
    }
    
    // Test 5: Try different time format (absolute timestamps)
    println!("\n=== Test 5: Using absolute timestamps ===");
    let abs_query = format!(
        "SELECT time, value FROM \"HeartRate\" WHERE time >= '{}' AND time <= '{}' LIMIT 10",
        start_time.to_rfc3339(),
        end_time.to_rfc3339()
    );
    println!("Query: {}", abs_query);
    
    match client.json_query(ReadQuery::new(abs_query)).await {
        Ok(result) => {
            println!("Absolute timestamp query successful!");
            println!("Result: {:#?}", result);
        }
        Err(e) => {
            println!("Failed to query with absolute timestamps: {}", e);
        }
    }
    
    Ok(())
}
