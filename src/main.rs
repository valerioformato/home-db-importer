use clap::{Parser, Subcommand};
mod csv_parser;
mod influx_client;
use csv_parser::CsvParser;
use influx_client::InfluxClient;
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
    Import {
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
    },

    /// Validate a CSV file format without importing
    Validate {
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
        Commands::Import {
            source,
            url,
            org,
            bucket,
            token,
            time_column,
            time_format,
            measurement,
            header_rows,
        } => {
            println!("Importing data from '{}' into InfluxDB", source);
            println!("  URL: {}", url);
            println!("  Organization: {}", org);
            println!("  Bucket: {}", bucket);
            println!("  Measurement: {}", measurement);
            println!("  Time column: {} (format: {})", time_column, time_format);
            println!("  Header rows: {}", header_rows);

            // Create parser with the specified header rows
            let parser = CsvParser::new(&source).with_header_rows(header_rows);

            // Parse the CSV data
            match parser.parse() {
                Ok(records) => {
                    println!("Successfully parsed {} records", records.len());

                    // Show a preview of the data before importing
                    match parser.format_parsed_data() {
                        Ok(preview) => {
                            println!("\nPreview of data to be imported:\n{}", preview);
                        }
                        Err(e) => {
                            eprintln!("Error generating preview: {}", e);
                        }
                    }

                    // Create InfluxDB client and import the data
                    let influx_client = InfluxClient::new(&url, &org, &bucket, &token);

                    match influx_client
                        .write_funds_records(&records, &measurement, &time_column, &time_format)
                        .await
                    {
                        Ok(count) => {
                            println!("Successfully imported {} data points to InfluxDB", count);
                        }
                        Err(e) => {
                            eprintln!("Error writing to InfluxDB: {}", e);
                            process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error parsing CSV data: {}", e);
                    process::exit(1);
                }
            }
        }

        Commands::Validate {
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

    // Debug info
    if cli.debug > 0 {
        println!("Debug mode is on (level: {})", cli.debug);
    }
}
