// Temporary file to hold the method implementation
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
