# Home DB Importer

[![Rust CI](https://github.com/yourusername/home-db-importer/actions/workflows/rust.yml/badge.svg)](https://github.com/yourusername/home-db-importer/actions/workflows/rust.yml)
[![Security Audit](https://github.com/yourusername/home-db-importer/actions/workflows/security-audit.yml/badge.svg)](https://github.com/yourusername/home-db-importer/actions/workflows/security-audit.yml)

A tool to import home data into InfluxDB from CSV files and health data from Health Connect SQLite exports.

## Features

- Parse CSV files with single or multi-row headers
- Import health data from Health Connect SQLite exports (heart rate, steps, sleep, weight)
- Validate CSV files before importing
- Import data into InfluxDB with efficient batch processing
- Configure via command line or configuration file
- Track import state to avoid reimporting the same data
- Dry-run mode for testing without writing to InfluxDB

## Installation

```bash
cargo install --git https://github.com/valerioformato/home-db-importer
```

## Usage

### Importing Financial Data

```bash
# Import financial data from CSV
home-db-importer import-funds --source data.csv --url http://localhost:8086 --org myorg --bucket mybucket --token mytoken --measurement home_data
```

### Importing Health Data

```bash
# Import health data from Health Connect SQLite export
home-db-importer import-health-data --source health_connect_export.db --url http://localhost:8086 --bucket health_data --token your_token --state-file health_import_state.json

# Test import in dry-run mode without writing to InfluxDB
home-db-importer import-health-data --source health_connect_export.db --url http://localhost:8086 --bucket health_data --token your_token --dry-run
```

### Validating CSV Files

```bash
# Validate a CSV file
home-db-importer validate-csv --source data.csv --details
```

## Supported Health Data Types

The following Health Connect data types are supported:

- Heart Rate
- Steps
- Sleep (with stage detection: AWAKE, LIGHT, DEEP, REM)
- Weight

## License

MIT
