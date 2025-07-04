use chrono::{DateTime, NaiveDateTime, Utc};
use clap::{Parser, Subcommand};
mod csv_parser;
mod health_data;
mod influx_client;
mod state_management;
use csv_parser::CsvParser;
use health_data::HealthDataReader;
use influx_client::InfluxClient;
use state_management::{load_import_state, save_import_state};
use std::collections::HashMap;
use std::process;

#[derive(Parser)]
#[command(author, version, about = "Import home data into InfluxDB", long_about = None)]
struct Cli {
    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import data from a CSV file into InfluxDB
    ImportFunds {
        /// The CSV file to import
        #[arg(short, long, required = true)]
        source: String,

        /// InfluxDB URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        url: String,

        /// InfluxDB organization
        #[arg(short, long)]
        org: String,

        /// InfluxDB bucket/database
        #[arg(short, long)]
        bucket: String,

        /// InfluxDB token for authentication
        #[arg(short, long)]
        token: String,

        /// Timestamp column name in CSV
        #[arg(long, default_value = "timestamp")]
        time_column: String,

        /// Timestamp format (e.g., "YYYY-MM-DD HH:MM:SS")
        #[arg(long, default_value = "%Y-%m-%d %H:%M:%S")]
        time_format: String,

        /// Measurement name in InfluxDB
        #[arg(short, long, required = true)]
        measurement: String,

        /// Number of header rows in CSV file
        #[arg(long, default_value = "1")]
        header_rows: usize,

        /// Run in dry-run mode (don't write to InfluxDB, just show queries)
        #[arg(long)]
        dry_run: bool,

        /// State file to track last imported timestamp
        #[arg(long, default_value = ".import_state.json")]
        state_file: String,

        /// Force import all records, ignoring state file
        #[arg(long)]
        force_all: bool,
    },

    /// Import health data from a Health Connect SQLite export
    ImportHealthData {
        /// The SQLite database file to import
        #[arg(short, long, required = true)]
        source: String,

        /// InfluxDB URL
        #[arg(short, long, default_value = "http://localhost:8086")]
        url: String,

        /// InfluxDB organization
        #[arg(short, long)]
        org: String,

        /// InfluxDB bucket/database
        #[arg(short, long)]
        bucket: String,

        /// InfluxDB token for authentication
        #[arg(short, long)]
        token: String,

        /// State file to track last imported timestamp
        #[arg(long, default_value = ".health_import_state.json")]
        state_file: String,

        /// Force import all records, ignoring state file
        #[arg(long)]
        force_all: bool,

        /// Run in dry-run mode (don't write to InfluxDB, just show queries)
        #[arg(long)]
        dry_run: bool,

        /// Only import specific data types (comma-separated). Available: HeartRate,Steps,Sleep,Weight,TotalCalories,BasalMetabolicRate,BodyFat,ExerciseSession
        #[arg(long)]
        data_types: Option<String>,

        /// Enable heart rate gap-filling mode (checks InfluxDB for existing data in the last N days and fills gaps).
        /// Note: Gap-filling mode only imports heart rate data and does not update the state file.
        /// Run normal sync first to update state, then use gap-filling as a maintenance operation.
        #[arg(long)]
        gap_fill_heart_rate: Option<i64>,
    },

    /// Validate a CSV file format without importing
    ValidateCSV {
        /// The CSV file to validate
        #[arg(short, long)]
        source: String,

        /// Show detailed information about the CSV structure
        #[arg(short, long)]
        details: bool,

        /// Number of header rows in CSV file
        #[arg(long, default_value = "1")]
        header_rows: usize,
    },

    /// Generate a template configuration file
    Init {
        /// Output file for the configuration
        #[arg(short, long, default_value = "influx-import.toml")]
        output: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::ImportFunds {
            source,
            url,
            org,
            bucket,
            token,
            time_column,
            time_format,
            measurement,
            header_rows,
            dry_run,
            state_file,
            force_all,
        } => {
            println!("Importing funds data from '{}' into InfluxDB", source);
            println!("  URL: {}", url);
            println!("  Organization: {}", org);
            println!("  Bucket: {}", bucket);
            println!("  Measurement: {}", measurement);
            println!("  Time column: {} (format: {})", time_column, time_format);
            println!("  Header rows: {}", header_rows);
            println!("  Dry-run mode: {}", if dry_run { "ON" } else { "OFF" });
            println!("  State file: {}", state_file);

            // Load the import state
            let mut import_state = load_import_state(&state_file, &source);

            if force_all {
                println!("Force import all records (--force-all flag is set)");
                import_state.last_imported_timestamp = None;
            } else if let Some(timestamp) = import_state.last_imported_timestamp {
                println!("Skipping records before: {}", timestamp);
                println!(
                    "Previously imported: {} records",
                    import_state.records_imported
                );
            } else {
                println!("No previous import state found, importing all records");
            }

            // Create parser with the specified header rows
            let parser = CsvParser::new(&source).with_header_rows(header_rows);

            // Parse the CSV data
            match parser.parse() {
                Ok(records) => {
                    println!("Successfully parsed {} records", records.len());

                    // Filter records based on timestamp
                    let filtered_records = if let Some(last_ts) =
                        import_state.last_imported_timestamp
                    {
                        let filtered = records
                            .iter()
                            .filter(|record| {
                                // Only include records with timestamp greater than last imported
                                if let Some(time_idx) = record.column_indexes.get(&time_column) {
                                    if let Some(time_value) = record.values.get(*time_idx) {
                                        if let Ok(naive_dt) =
                                            NaiveDateTime::parse_from_str(time_value, &time_format)
                                        {
                                            let record_time: DateTime<Utc> =
                                                DateTime::from_naive_utc_and_offset(naive_dt, Utc);
                                            return record_time > last_ts;
                                        }
                                    }
                                }
                                // If timestamp can't be parsed, include the record to be safe
                                true
                            })
                            .cloned()
                            .collect::<Vec<_>>();

                        println!(
                            "Filtered from {} to {} records (skipping previously imported)",
                            records.len(),
                            filtered.len()
                        );
                        filtered
                    } else {
                        records.clone()
                    };

                    if filtered_records.is_empty() {
                        println!("No new records to import");
                        return;
                    }

                    // Show a preview of the filtered data before importing
                    println!(
                        "\nPreview of data to be imported: {} records",
                        filtered_records.len()
                    );

                    // Try to find the latest timestamp from the records we're about to import
                    let mut latest_timestamp: Option<DateTime<Utc>> = None;
                    for record in &filtered_records {
                        if let Some(time_idx) = record.column_indexes.get(&time_column) {
                            if let Some(time_value) = record.values.get(*time_idx) {
                                if let Ok(naive_dt) =
                                    NaiveDateTime::parse_from_str(time_value, &time_format)
                                {
                                    let record_time =
                                        DateTime::from_naive_utc_and_offset(naive_dt, Utc);
                                    if latest_timestamp.is_none()
                                        || Some(record_time) > latest_timestamp
                                    {
                                        latest_timestamp = Some(record_time);
                                    }
                                }
                            }
                        }
                    }

                    if dry_run {
                        println!("Dry-run mode enabled. No data will be written to InfluxDB.");

                        // Create InfluxDB client in dry-run mode
                        let influx_client = InfluxClient::new_dry_run(&url, &bucket, &token);

                        match influx_client
                            .write_funds_records(&filtered_records, &time_column, &time_format)
                            .await
                        {
                            Ok(count) => {
                                println!("Dry run complete: {} data points would have been sent to InfluxDB", count);

                                // Update the import state but don't save it in dry run mode
                                println!("In a real import, would update the state file with latest timestamp: {:?}", latest_timestamp);
                            }
                            Err(e) => {
                                eprintln!("Error in dry-run: {}", e);
                                process::exit(1);
                            }
                        }
                    } else {
                        // Create InfluxDB client and import the data
                        let influx_client = InfluxClient::new(&url, &bucket, &token);

                        match influx_client
                            .write_funds_records(&filtered_records, &time_column, &time_format)
                            .await
                        {
                            Ok(count) => {
                                println!("Successfully imported {} data points to InfluxDB", count);

                                // Update the import state
                                if let Some(ts) = latest_timestamp {
                                    import_state.last_imported_timestamp = Some(ts);
                                    import_state.records_imported += filtered_records.len();

                                    // Save the updated state
                                    match save_import_state(&import_state, &state_file) {
                                        Ok(_) => {
                                            println!("Updated import state saved to {}", state_file)
                                        }
                                        Err(e) => eprintln!("Failed to save import state: {}", e),
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error writing to InfluxDB: {}", e);
                                process::exit(1);
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing CSV data: {}", e);
                    process::exit(1);
                }
            }
        }

        Commands::ImportHealthData {
            source,
            url,
            bucket,
            org,
            token,
            state_file,
            force_all,
            dry_run,
            data_types,
            gap_fill_heart_rate,
        } => {
            println!("Importing health data from SQLite database: '{}'", source);
            println!("  URL: {}", url);
            println!("  Organization: {}", org);
            println!("  Bucket: {}", bucket);
            println!("  Dry-run mode: {}", if dry_run { "ON" } else { "OFF" });
            println!("  State file: {}", state_file);

            // Parse data types filter if provided
            let requested_data_types = if let Some(data_types_str) = data_types {
                let types: Vec<String> = data_types_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                println!("  Data types filter: {:?}", types);
                Some(types)
            } else {
                println!("  Data types filter: All types");
                None
            };

            // Load the import state
            let mut import_state = load_import_state(&state_file, &source);

            if force_all {
                println!("Force import all records (--force-all flag is set)");
                import_state.last_imported_timestamp = None;
            } else if let Some(timestamp) = import_state.last_imported_timestamp {
                println!("Skipping records before: {}", timestamp);
                println!(
                    "Previously imported: {} records",
                    import_state.records_imported
                );
            } else {
                println!("No previous import state found, importing all records");
            }

            // Create a HealthDataReader to read from the SQLite database
            let reader = HealthDataReader::new(&source);

            // Validate the database structure
            match reader.validate_db() {
                Ok(validation_info) => {
                    println!("Database validation successful");
                    println!("{}", validation_info);
                }
                Err(e) => {
                    eprintln!("Failed to validate database: {}", e);
                    process::exit(1);
                }
            }

            // Create InfluxDB client early for gap-filling functionality
            let influx_client = if dry_run {
                InfluxClient::new_dry_run(&url, &bucket, &token)
            } else {
                InfluxClient::new(&url, &bucket, &token)
            };

            // Get health data since the last import timestamp
            println!("Retrieving health data...");
            let mut records_map = if let Some(_days_back) = gap_fill_heart_rate {
                // Gap-filling mode: Only process heart rate data
                println!("Gap-filling mode: Only importing heart rate data (assuming other data types are already synced)");
                HashMap::new() // Start with empty map, will be populated by gap-filling
            } else if let Some(data_types_filter) = requested_data_types {
                // Use filtered retrieval
                match reader.get_filtered_health_data_since(
                    import_state.last_imported_timestamp,
                    &data_types_filter,
                ) {
                    Ok(records) => records,
                    Err(e) => {
                        eprintln!("Error retrieving filtered health data: {}", e);
                        process::exit(1);
                    }
                }
            } else {
                // Get all data types
                match reader.get_all_health_data_since(import_state.last_imported_timestamp) {
                    Ok(records) => records,
                    Err(e) => {
                        eprintln!("Error retrieving health data: {}", e);
                        process::exit(1);
                    }
                }
            };

            // Handle heart rate gap-filling if requested
            if let Some(days_back) = gap_fill_heart_rate {
                println!(
                    "\nHeart rate gap-filling enabled for the last {} days",
                    days_back
                );
                println!("📋 Gap-filling mode: Only heart rate data will be imported");
                println!("   (Other data types assumed to be already synced)");

                match reader
                    .get_heart_rate_with_gap_filling(&influx_client, days_back)
                    .await
                {
                    Ok(gap_fill_records) => {
                        if !gap_fill_records.is_empty() {
                            println!(
                                "✅ Adding {} gap-filled heart rate records",
                                gap_fill_records.len()
                            );
                            // Add only the heart rate records with gap-filled data
                            records_map.insert("HeartRate".to_string(), gap_fill_records);
                        } else {
                            println!("✅ No heart rate gaps found - all data is up to date");
                            // Keep records_map empty since no gaps were found
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Heart rate gap-filling failed: {}", e);
                        process::exit(1);
                    }
                }
            }

            // Count total records
            let total_records: usize = records_map.values().map(|v| v.len()).sum();

            if total_records == 0 {
                println!("No new health records to import");
                return;
            }

            println!("Found {} health records to import:", total_records);
            for (record_type, records) in &records_map {
                println!("  - {}: {} records", record_type, records.len());
            }

            // Find the latest timestamp across all records
            let mut latest_timestamp: Option<DateTime<Utc>> = None;
            for records in records_map.values() {
                for record in records {
                    if latest_timestamp.is_none() || Some(record.timestamp) > latest_timestamp {
                        latest_timestamp = Some(record.timestamp);
                    }
                }
            }

            // Write the health records to InfluxDB
            match influx_client.write_health_records(&records_map).await {
                Ok(count) => {
                    let mode_prefix = if dry_run {
                        "Would have"
                    } else {
                        "Successfully"
                    };
                    println!(
                        "{} imported {} health data points to InfluxDB",
                        mode_prefix, count
                    );

                    // Update and save the import state (unless in dry-run mode or gap-filling mode)
                    if !dry_run && gap_fill_heart_rate.is_none() {
                        if let Some(ts) = latest_timestamp {
                            import_state.last_imported_timestamp = Some(ts);
                            import_state.records_imported += total_records;

                            // Save the updated state
                            match save_import_state(&import_state, &state_file) {
                                Ok(_) => {
                                    println!("Updated import state saved to {}", state_file)
                                }
                                Err(e) => eprintln!("Failed to save import state: {}", e),
                            }
                        }
                    } else if dry_run {
                        println!("Dry-run mode: State file not updated");
                        if let Some(ts) = latest_timestamp {
                            println!("Would update last imported timestamp to: {}", ts);
                        }
                    } else if gap_fill_heart_rate.is_some() {
                        println!("Gap-filling mode: State file not updated");
                        println!("💡 Gap-filling is a maintenance operation - run normal sync first to update state");
                        if let Some(ts) = latest_timestamp {
                            println!("Latest gap-filled timestamp: {}", ts);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error writing health data to InfluxDB: {}", e);
                    process::exit(1);
                }
            }
        }

        Commands::ValidateCSV {
            source,
            details,
            header_rows,
        } => {
            println!("Validating CSV file: '{}'", source);
            println!("  Header rows: {}", header_rows);

            // Show information about the details flag
            if details {
                println!("Details mode: ON - Will show all CSV records");
            } else {
                println!("Details mode: OFF - Use --details flag to see full CSV content");
            }

            // Create parser with specified number of header rows
            let parser = CsvParser::new(&source).with_header_rows(header_rows);

            match parser.validate(details) {
                Ok(report) => {
                    println!("{}", report);
                }
                Err(e) => {
                    eprintln!("Validation error: {}", e);
                    process::exit(1);
                }
            }
        }

        Commands::Init { output } => {
            println!("Generating template configuration file: '{}'", output);
            // Generate a template configuration file
        }
    }

    if cli.debug > 0 { // Debug info        println!("Debug mode is on (level: {})", cli.debug);
    }
}
