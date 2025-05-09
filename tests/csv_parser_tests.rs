use home_db_importer::csv_parser::{CsvParser, CsvRecord};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::{tempdir, TempDir};

// We need to keep the TempDir alive for the duration of the test
struct TestFile {
    path: PathBuf,
    _temp_dir: TempDir, // The underscore prevents "unused variable" warnings
}

fn create_test_csv(content: &str) -> TestFile {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.csv");

    let mut file = File::create(&file_path).unwrap();
    file.write_all(content.as_bytes()).unwrap();

    TestFile {
        path: file_path,
        _temp_dir: temp_dir,
    }
}

#[test]
fn test_parser_with_empty_file() {
    let test_file = create_test_csv("");
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    assert!(parser.file_exists());
    let result = parser.parse();
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[test]
fn test_parser_with_header_only() {
    let test_file = create_test_csv("name,age,city\n");
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    assert!(parser.file_exists());
    let result = parser.parse();
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0); // No data rows, just header
}

#[test]
fn test_parser_with_single_header_row() {
    let content = "name,age,city\nJohn,30,New York\nJane,25,Boston\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    assert!(parser.file_exists());
    let result = parser.parse();
    assert!(result.is_ok());

    let records = result.unwrap();
    assert_eq!(records.len(), 2); // Two data rows

    // First record should have values from the first data row
    let first_record = &records[0];

    // Find the column indices
    let name_idx = first_record.column_indexes.get("name").unwrap();
    let age_idx = first_record.column_indexes.get("age").unwrap();
    let city_idx = first_record.column_indexes.get("city").unwrap();

    // Check values using indices
    assert_eq!(first_record.values[*name_idx], "John");
    assert_eq!(first_record.values[*age_idx], "30");
    assert_eq!(first_record.values[*city_idx], "New York");
}

#[test]
fn test_parser_with_multi_header_rows() {
    // CSV with two header rows that should be joined with a dot
    let content = "sensor,sensor,sensor\ntemp,humidity,pressure\n22.5,45,1013\n23.1,48,1014\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap()).with_header_rows(2);

    assert!(parser.file_exists());
    let result = parser.parse();
    assert!(result.is_ok());

    let records = result.unwrap();
    assert_eq!(records.len(), 2); // Two data rows

    // Check that the headers were properly combined
    let first_record = &records[0];

    let temp_idx = first_record.column_indexes.get("sensor.temp").unwrap();
    let humidity_idx = first_record.column_indexes.get("sensor.humidity").unwrap();
    let pressure_idx = first_record.column_indexes.get("sensor.pressure").unwrap();

    assert_eq!(first_record.values[*temp_idx], "22.5");
    assert_eq!(first_record.values[*humidity_idx], "45");
    assert_eq!(first_record.values[*pressure_idx], "1013");
}

#[test]
fn test_header_with_spaces() {
    // CSV with header containing spaces that should be replaced with underscores
    let content = "First Name,Last Name,Home City\nJohn,Doe,New York\nJane,Smith,Boston\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    assert!(parser.file_exists());
    let result = parser.parse();
    assert!(result.is_ok());

    let records = result.unwrap();
    assert_eq!(records.len(), 2); // Two data rows

    // Check that spaces in headers were replaced with underscores
    let first_record = &records[0];

    let first_name_idx = first_record.column_indexes.get("First_Name").unwrap();
    let last_name_idx = first_record.column_indexes.get("Last_Name").unwrap();
    let home_city_idx = first_record.column_indexes.get("Home_City").unwrap();

    assert_eq!(first_record.values[*first_name_idx], "John");
    assert_eq!(first_record.values[*last_name_idx], "Doe");
    assert_eq!(first_record.values[*home_city_idx], "New York");
}

#[test]
fn test_validation_with_details() {
    let content = "date,name,age,city\n,John,30,New York\n,Jane,25,Boston\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    let result = parser.validate(true);
    assert!(result.is_ok());

    let validation_output = result.unwrap();

    // Check basic validation info
    assert!(validation_output.contains("Validating CSV file:"));
    assert!(validation_output.contains("Total rows: 3"));
    assert!(validation_output.contains("Header rows: 1"));
    assert!(validation_output.contains("Data rows: 2"));

    // Check detailed information is included
    assert!(validation_output.contains("Parsed Data Details:"));
    assert!(validation_output.contains("Found 2 records with 4 columns"));

    // Check that all expected headers are present, without requiring specific order
    assert!(validation_output.contains("Headers:"));
    assert!(validation_output.contains("name"));
    assert!(validation_output.contains("age"));
    assert!(validation_output.contains("city"));

    // Check sample data is shown
    assert!(validation_output.contains("Sample data:"));
    assert!(validation_output.contains("Record 1:"));
    assert!(validation_output.contains("name: John"));
    assert!(validation_output.contains("age: 30"));
    assert!(validation_output.contains("city: New York"));
    assert!(validation_output.contains("Record 2:"));
    assert!(validation_output.contains("name: Jane"));
    assert!(validation_output.contains("age: 25"));
    assert!(validation_output.contains("city: Boston"));
}

#[test]
fn test_validation_without_details() {
    let content = "name,age,city\nJohn,30,New York\nJane,25,Boston\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    let result = parser.validate(false);
    assert!(result.is_ok());

    let validation_output = result.unwrap();

    // Check basic validation info is included
    assert!(validation_output.contains("Validating CSV file:"));
    assert!(validation_output.contains("Total rows: 3"));
    assert!(validation_output.contains("Header rows: 1"));
    assert!(validation_output.contains("Data rows: 2"));

    // Check detailed information is NOT included
    assert!(!validation_output.contains("Parsed Data Details:"));
    assert!(!validation_output.contains("Found 2 records with 3 columns"));
    assert!(!validation_output.contains("Sample data:"));
    assert!(!validation_output.contains("Record 1:"));
    assert!(!validation_output.contains("name: John"));
}

#[test]
fn test_validation_with_empty_file() {
    let test_file = create_test_csv("");
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    let result = parser.validate(true);
    assert!(result.is_ok());

    let validation_output = result.unwrap();
    assert!(validation_output.contains("Total rows: 0"));
    assert!(validation_output.contains("Header rows: 1"));
    assert!(validation_output.contains("Data rows: 0"));
    assert!(validation_output.contains("No data found in CSV file."));
}

#[test]
fn test_validation_with_multi_header_rows() {
    // CSV with two header rows that should be joined with a dot
    let content =
        ",sensor,sensor,sensor\ntimestamp,temp,humidity,pressure\n,22.5,45,1013\n,23.1,48,1014\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap()).with_header_rows(2);

    let result = parser.validate(true);
    assert!(result.is_ok());

    let validation_output = result.unwrap();
    assert!(validation_output.contains("Total rows: 4"));
    assert!(validation_output.contains("Header rows: 2"));
    assert!(validation_output.contains("Data rows: 2"));
    // assert!(validation_output
    //     .contains("Headers: timestamp, sensor.temp, sensor.humidity, sensor.pressure"));
    assert!(validation_output.contains("sensor.temp: 22.5"));
    assert!(validation_output.contains("sensor.humidity: 45"));
}

#[test]
fn test_format_parsed_data() {
    let content = "date,name,age,city\n,John,30,New York\n,Jane,25,Boston\n";
    let test_file = create_test_csv(content);
    let parser = CsvParser::new(test_file.path.to_str().unwrap());

    let parse_result = parser.parse().unwrap();
    let result = parser.format_parsed_data();
    assert!(result.is_ok());

    let formatted = result.unwrap();
    assert!(formatted.contains("Found 2 records with 4 columns"));
    // assert!(formatted.contains("Headers: name, age, city"));
    assert!(formatted.contains("Record 1:"));
    assert!(formatted.contains("name: John"));
    assert!(formatted.contains("name: Jane"));
    assert!(formatted.contains("city: Boston"));
}
