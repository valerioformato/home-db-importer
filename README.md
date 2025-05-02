# Home DB Importer

[![Rust CI](https://github.com/yourusername/home-db-importer/actions/workflows/rust.yml/badge.svg)](https://github.com/yourusername/home-db-importer/actions/workflows/rust.yml)
[![Security Audit](https://github.com/yourusername/home-db-importer/actions/workflows/security-audit.yml/badge.svg)](https://github.com/yourusername/home-db-importer/actions/workflows/security-audit.yml)

A tool to import home data into InfluxDB from CSV files.

## Features

- Parse CSV files with single or multi-row headers
- Validate CSV files before importing
- Import data into InfluxDB with proper timestamp handling
- Configure via command line or configuration file

## Installation

```bash
cargo install --git https://github.com/valerioformato/home-db-importer
```

## Usage

```bash
# Import data
home-db-importer import --source data.csv --org myorg --bucket mybucket --token mytoken --measurement home_data

# Validate a CSV file
home-db-importer validate --source data.csv --details

# Generate a configuration template
home-db-importer init --output config.toml
```

## License

MIT
