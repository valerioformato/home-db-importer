use csv::{ReaderBuilder, StringRecord};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::path::Path;

/// Represents a parser for CSV files
pub struct CsvParser {
    file_path: String,
    header_rows: usize,
    time_column_index: Option<usize>, // Typically the first column (0)
}

/// Represents a parsed CSV record
#[derive(Clone, Debug)]
pub struct CsvRecord {
    pub header_values: Vec<Vec<String>>, // Matrix of header values [row][column]
    pub column_indexes: HashMap<String, usize>, // Map column identifier to index
    pub values: Vec<String>,             // Raw values for this record
    pub time_column_index: Option<usize>, // Index of the time column
}

impl CsvRecord {
    /// Gets the timestamp value from the record
    pub fn get_time_value(&self) -> Option<&str> {
        if let Some(idx) = self.time_column_index {
            if idx < self.values.len() {
                return Some(&self.values[idx]);
            }
        }
        None
    }

    /// Gets a measurement value for a specific column by name
    pub fn get_measurement_value(&self, column_name: &str) -> Option<&str> {
        if let Some(idx) = self.column_indexes.get(column_name) {
            if *idx < self.values.len() {
                return Some(&self.values[*idx]);
            }
        }
        None
    }

    /// Gets all measurement columns (excluding the time column)
    pub fn get_measurement_columns(&self) -> Vec<&String> {
        self.column_indexes
            .keys()
            .filter(|&k| {
                if let Some(idx) = self.time_column_index {
                    self.column_indexes.get(k) != Some(&idx)
                } else {
                    true
                }
            })
            .collect()
    }
}

impl fmt::Display for CsvRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Record:")?;

        // Show the timestamp first if it exists
        if let Some(time_idx) = self.time_column_index {
            if let Some(time_col) = self.column_indexes.iter().find(|(_, &idx)| idx == time_idx) {
                if let Some(time_value) = self.values.get(time_idx) {
                    writeln!(f, "  Timestamp ({}): {}", time_col.0, time_value)?;
                }
            }
        }

        // Then show all other columns
        for (header, index) in &self.column_indexes {
            if Some(*index) != self.time_column_index {
                if let Some(value) = self.values.get(*index) {
                    writeln!(f, "  {}: {}", header, value)?;
                }
            }
        }
        Ok(())
    }
}

impl CsvParser {
    /// Creates a new CSV parser for the given file path
    pub fn new(file_path: &str) -> Self {
        CsvParser {
            file_path: file_path.to_string(),
            header_rows: 1,             // Default to 1 header row
            time_column_index: Some(0), // Default to first column as timestamp
        }
    }

    /// Sets the number of rows that make up the header
    pub fn with_header_rows(mut self, rows: usize) -> Self {
        self.header_rows = rows;
        self
    }

    /// Sets the column index to use as the timestamp
    /// Use None to indicate there is no timestamp column
    pub fn with_time_column_index(mut self, index: Option<usize>) -> Self {
        self.time_column_index = index;
        self
    }

    /// Gets the number of header rows
    #[allow(dead_code)]
    pub fn header_rows(&self) -> usize {
        self.header_rows
    }

    /// Gets the time column index
    pub fn time_column_index(&self) -> Option<usize> {
        self.time_column_index
    }

    /// Checks if the file exists
    pub fn file_exists(&self) -> bool {
        Path::new(&self.file_path).exists()
    }

    /// Process header rows to create column names
    fn process_headers(&self, headers: &[StringRecord]) -> Vec<String> {
        if headers.is_empty() {
            return Vec::new();
        }

        let mut column_headers = Vec::new();

        // If we only have one header row, use it directly
        if headers.len() == 1 {
            for field in headers[0].iter() {
                // Clean up header: replace spaces with underscores and remove newlines
                let clean_header = field.replace(' ', "_").replace(['\n', '\r'], "");
                column_headers.push(clean_header);
            }
            return column_headers;
        }

        // If we have multiple header rows, combine them
        let columns = headers[0].len();
        for col in 0..columns {
            let mut parts = Vec::new();

            for row in headers {
                if col < row.len() {
                    // Clean up the header part: remove newlines
                    let clean_part = row[col].replace(['\n', '\r'], "").trim().to_string();

                    // Only add non-empty parts
                    if !clean_part.is_empty() {
                        parts.push(clean_part);
                    }
                }
            }

            // Create the header
            let header = if parts.is_empty() {
                // If all parts were empty, use a default column name
                format!("column_{}", col + 1)
            } else {
                // Join parts in a deterministic order (just as they appear in the CSV)
                parts.join(".")
            };

            // Replace spaces with underscores
            let final_header = header.replace(' ', "_");
            column_headers.push(final_header);
        }

        column_headers
    }

    /// Parse the CSV file and return the records
    pub fn parse(&self) -> Result<Vec<CsvRecord>, Box<dyn Error>> {
        // Check if file exists before attempting to parse
        if !self.file_exists() {
            return Err(format!("File does not exist: {}", self.file_path).into());
        }

        // Open the file
        let file = File::open(&self.file_path)?;

        // Create CSV reader with flexible configuration
        let mut rdr = ReaderBuilder::new()
            .has_headers(false) // We'll handle headers manually
            .flexible(true) // Allow rows with different column counts
            .from_reader(file);

        let mut records = Vec::new();
        let mut header_rows = Vec::new();

        // Read header rows
        for _ in 0..self.header_rows {
            if let Some(result) = rdr.records().next() {
                let record = result?;
                header_rows.push(record);
            } else {
                // Not enough rows in the file
                break;
            }
        }

        // Process headers to create column names
        let headers = self.process_headers(&header_rows);

        // If file only has headers or is empty, return empty records
        if headers.is_empty() {
            return Ok(records);
        }

        // Create a new reader to start from the beginning
        let file = File::open(&self.file_path)?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true) // Allow flexibility for rows with different column counts
            .from_reader(file);

        // Skip header rows
        let mut reader = rdr.records();
        for _ in 0..self.header_rows {
            if reader.next().is_none() {
                break;
            }
        }

        // Store header values as strings for easier handling in InfluxDB client
        let header_values: Vec<Vec<String>> = header_rows
            .iter()
            .map(|row| row.iter().map(|field| field.to_string()).collect())
            .collect();

        // Build column index mapping
        let mut column_indexes = HashMap::new();
        for (i, name) in headers.iter().enumerate() {
            column_indexes.insert(name.clone(), i);
        }

        // Read data rows
        for result in reader {
            let record = result?;
            let values: Vec<String> = record.iter().map(|field| field.to_string()).collect();

            records.push(CsvRecord {
                header_values: header_values.clone(),
                column_indexes: column_indexes.clone(),
                values,
                time_column_index: self.time_column_index,
            });
        }

        Ok(records)
    }

    /// Generates a formatted string representation of the parsed CSV data
    pub fn format_parsed_data(&self) -> Result<String, Box<dyn Error>> {
        let records = self.parse()?;

        if records.is_empty() {
            return Ok("No data found in CSV file.".to_string());
        }

        let mut output = String::new();
        output.push_str(&format!(
            "Found {} records with {} columns\n",
            records.len(),
            records[0].column_indexes.len()
        ));

        // Show which column is the timestamp column, if any
        if let Some(time_idx) = records[0].time_column_index {
            // Find the column name for the timestamp
            let unknown = "unknown".to_string();
            let time_column_name = records[0]
                .column_indexes
                .iter()
                .find_map(|(key, &idx)| if idx == time_idx { Some(key) } else { None })
                .unwrap_or(&unknown);

            output.push_str(&format!(
                "Timestamp column: {} (index {})\n",
                time_column_name, time_idx
            ));
        }

        output.push_str("Headers: ");
        output.push_str(
            &records[0]
                .column_indexes
                .keys()
                .cloned()
                .collect::<Vec<String>>()
                .join(", "),
        );
        output.push_str("\n\nSample data:\n");

        // Show up to 5 records as samples
        let sample_size = std::cmp::min(5, records.len());
        for (i, record) in records.iter().take(sample_size).enumerate() {
            output.push_str(&format!("\nRecord {}:\n", i + 1));

            // Show the timestamp first if it exists
            if let Some(time_value) = record.get_time_value() {
                if let Some(time_idx) = record.time_column_index {
                    if let Some((time_col, _)) = record
                        .column_indexes
                        .iter()
                        .find(|(_, &idx)| idx == time_idx)
                    {
                        output.push_str(&format!("  Timestamp ({}): {}\n", time_col, time_value));
                    }
                }
            }

            // Then show all other columns
            for (header, index) in &record.column_indexes {
                if Some(*index) != record.time_column_index {
                    if let Some(value) = record.values.get(*index) {
                        output.push_str(&format!("  {}: {}\n", header, value));
                    }
                }
            }
        }

        if records.len() > sample_size {
            output.push_str(&format!(
                "\n... and {} more records\n",
                records.len() - sample_size
            ));
        }

        Ok(output)
    }

    /// Validates a CSV file and returns a formatted report
    pub fn validate(&self, show_details: bool) -> Result<String, Box<dyn Error>> {
        if !self.file_exists() {
            return Err(format!("File does not exist: {}", self.file_path).into());
        }

        let mut output = String::new();
        output.push_str(&format!("Validating CSV file: {}\n", self.file_path));

        // Check if file can be opened
        let file = File::open(&self.file_path)?;

        // Create CSV reader
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

        // Count total rows
        let mut row_count = 0;
        for result in rdr.records() {
            let _ = result?; // Just checking if we can read each record
            row_count += 1;
        }

        // Calculate data rows (total rows minus header rows)
        let data_rows = if row_count >= self.header_rows {
            row_count - self.header_rows
        } else {
            0
        };

        output.push_str(&format!("Total rows: {}\n", row_count));
        output.push_str(&format!("Header rows: {}\n", self.header_rows));
        output.push_str(&format!("Data rows: {}\n", data_rows));

        // If show_details is true, show the parsed data
        if show_details {
            output.push_str("\nParsed Data Details:\n");

            // Parse and show all the CSV content
            let records = self.parse()?;

            if records.is_empty() {
                output.push_str("No data found in CSV file.\n");
            } else {
                output.push_str(&format!(
                    "Found {} records with {} columns\n",
                    records.len(),
                    records[0].column_indexes.len()
                ));

                // Show which column is the timestamp column, if any
                if let Some(time_idx) = records[0].time_column_index {
                    // Find the column name for the timestamp
                    let unknown = "unknown".to_string();
                    let time_column_name = records[0]
                        .column_indexes
                        .iter()
                        .find_map(|(key, &idx)| if idx == time_idx { Some(key) } else { None })
                        .unwrap_or(&unknown);

                    output.push_str(&format!(
                        "Timestamp column: {} (index {})\n",
                        time_column_name, time_idx
                    ));
                }

                output.push_str("Headers: ");
                output.push_str(
                    &records[0]
                        .column_indexes
                        .keys()
                        .cloned()
                        .collect::<Vec<String>>()
                        .join(", "),
                );

                // Add "Sample data:" text that the test is looking for
                output.push_str("\n\nSample data:\n");

                // Show all records when details flag is on
                for (i, record) in records.iter().enumerate() {
                    output.push_str(&format!("\nRecord {}:\n", i + 1));

                    // Show the timestamp first if it exists
                    if let Some(time_value) = record.get_time_value() {
                        if let Some(time_idx) = record.time_column_index {
                            if let Some((time_col, _)) = record
                                .column_indexes
                                .iter()
                                .find(|(_, &idx)| idx == time_idx)
                            {
                                output.push_str(&format!(
                                    "  Timestamp ({}): {}\n",
                                    time_col, time_value
                                ));
                            }
                        }
                    }

                    // Then show all other columns
                    for (header, index) in &record.column_indexes {
                        if Some(*index) != record.time_column_index {
                            if let Some(value) = record.values.get(*index) {
                                output.push_str(&format!("  {}: {}\n", header, value));
                            }
                        }
                    }
                }
            }
        } else {
            // For non-detailed output, just provide a summary
            let records = self.parse()?;

            if records.is_empty() {
                output.push_str("\nNo data found in CSV file.\n");
            } else {
                output.push_str(&format!(
                    "\nParsed {} records with {} columns\n",
                    records.len(),
                    records[0].column_indexes.len()
                ));

                // Show which column is the timestamp column, if any
                if let Some(time_idx) = records[0].time_column_index {
                    // Find the column name for the timestamp
                    let unknown = "unknown".to_string();
                    let time_column_name = records[0]
                        .column_indexes
                        .iter()
                        .find_map(|(key, &idx)| if idx == time_idx { Some(key) } else { None })
                        .unwrap_or(&unknown);

                    output.push_str(&format!(
                        "Timestamp column: {} (index {})\n",
                        time_column_name, time_idx
                    ));
                }

                output.push_str("Headers: ");
                output.push_str(
                    &records[0]
                        .column_indexes
                        .keys()
                        .cloned()
                        .collect::<Vec<String>>()
                        .join(", "),
                );
                output.push_str("\n\nUse --details flag to see the full CSV content\n");
            }
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_new_parser() {
        let parser = CsvParser::new("test_file.csv");
        assert_eq!(parser.file_path, "test_file.csv");
        assert_eq!(parser.header_rows(), 1); // Default is 1 header row
        assert_eq!(parser.time_column_index(), Some(0)); // Default is first column as timestamp
    }

    #[test]
    fn test_with_header_rows() {
        let parser = CsvParser::new("test_file.csv").with_header_rows(2);
        assert_eq!(parser.header_rows(), 2);
    }

    #[test]
    fn test_with_time_column_index() {
        let parser = CsvParser::new("test_file.csv").with_time_column_index(Some(1));
        assert_eq!(parser.time_column_index(), Some(1));
    }

    #[test]
    fn test_file_exists_nonexistent_file() {
        let parser = CsvParser::new("nonexistent_file.csv");
        assert!(!parser.file_exists());
    }

    #[test]
    fn test_file_exists_real_file() {
        // Create a real temporary file
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let parser = CsvParser::new(path);
        assert!(parser.file_exists());
    }

    #[test]
    fn test_process_headers_with_newlines() {
        // Create a CSV parser
        let parser = CsvParser::new("test.csv");

        // Create a StringRecord with newlines in headers
        let record = StringRecord::from(vec!["Header1\nPart2", "Header2\r\nPart2", "Header\r3"]);
        let headers = vec![record];

        // Process the headers
        let processed = parser.process_headers(&headers);

        // Check that newlines were removed
        assert_eq!(processed, vec!["Header1Part2", "Header2Part2", "Header3"]);
    }

    #[test]
    fn test_process_multirow_headers_with_newlines() {
        // Create a CSV parser
        let parser = CsvParser::new("test.csv");

        // Create multiple StringRecords with newlines
        let record1 = StringRecord::from(vec!["Header\n1", "Header\r\n2", "Header 3"]);
        let record2 = StringRecord::from(vec!["Sub\r1", "Sub\n2", "Sub 3"]);
        let headers = vec![record1, record2];

        // Process the headers
        let processed = parser.process_headers(&headers);

        // Check that newlines were removed and spaces replaced with underscores
        assert_eq!(
            processed,
            vec!["Header1.Sub1", "Header2.Sub2", "Header_3.Sub_3"]
        );
    }

    #[test]
    fn test_process_headers_with_empty_cells() {
        // Create a CSV parser
        let parser = CsvParser::new("test.csv");

        // Create multiple StringRecords with some empty cells
        let record1 = StringRecord::from(vec!["Header1", "", "Header3"]);
        let record2 = StringRecord::from(vec!["Sub1", "Sub2", "Sub3"]);
        let headers = vec![record1, record2];

        // Process the headers
        let processed = parser.process_headers(&headers);

        // Check that empty cells are handled correctly (no leading dots)
        assert_eq!(processed, vec!["Header1.Sub1", "Sub2", "Header3.Sub3"]);
    }

    #[test]
    fn test_process_headers_all_empty_cell() {
        // Create a CSV parser
        let parser = CsvParser::new("test.csv");

        // Create multiple StringRecords with a completely empty column
        let record1 = StringRecord::from(vec!["Header1", "", "Header3"]);
        let record2 = StringRecord::from(vec!["Sub1", "", "Sub3"]);
        let headers = vec![record1, record2];

        // Process the headers
        let processed = parser.process_headers(&headers);

        // Check that completely empty cells get default names
        assert_eq!(processed, vec!["Header1.Sub1", "column_2", "Header3.Sub3"]);
    }

    #[test]
    fn test_parse_with_empty_header_cells() {
        // Create a temporary CSV file with empty cells in headers
        let mut temp_file = NamedTempFile::new().unwrap();

        writeln!(temp_file, "First,  ,Third").unwrap();
        writeln!(temp_file, "Sub1,Sub2,Sub3").unwrap();
        writeln!(temp_file, "value1,value2,value3").unwrap();
        writeln!(temp_file, "value4,value5,value6").unwrap();

        let path = temp_file.path().to_str().unwrap();

        // Parse the CSV file with 2 header rows
        let parser = CsvParser::new(path).with_header_rows(2);
        let records = parser.parse().unwrap();

        // Check that the headers were correctly processed
        assert_eq!(records.len(), 2);

        // Collect and sort headers to ensure consistent order for testing
        let mut headers: Vec<_> = records[0].column_indexes.keys().cloned().collect();
        headers.sort();

        assert_eq!(headers, vec!["First.Sub1", "Sub2", "Third.Sub3"]);

        // Check that the values were correctly assigned
        assert_eq!(
            records[0].values[records[0].column_indexes["First.Sub1"]],
            "value1"
        );
        assert_eq!(
            records[0].values[records[0].column_indexes["Sub2"]],
            "value2"
        );
        assert_eq!(
            records[0].values[records[0].column_indexes["Third.Sub3"]],
            "value3"
        );
    }

    #[test]
    fn test_parse_with_time_column() {
        // Create a temporary CSV file with a timestamp column
        let mut temp_file = NamedTempFile::new().unwrap();

        writeln!(temp_file, "Timestamp,Value1,Value2").unwrap();
        writeln!(temp_file, "2023-01-01T00:00:00Z,100,200").unwrap();
        writeln!(temp_file, "2023-01-01T01:00:00Z,110,210").unwrap();

        let path = temp_file.path().to_str().unwrap();

        // Parse the CSV file with 1 header row and timestamp column
        let parser = CsvParser::new(path)
            .with_header_rows(1)
            .with_time_column_index(Some(0));
        let records = parser.parse().unwrap();

        // Check that the timestamp column was correctly identified
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get_time_value(), Some("2023-01-01T00:00:00Z"));
        assert_eq!(records[1].get_time_value(), Some("2023-01-01T01:00:00Z"));

        // Check that the values were correctly assigned
        assert_eq!(
            records[0].values[records[0].column_indexes["Value1"]],
            "100"
        );
        assert_eq!(
            records[0].values[records[0].column_indexes["Value2"]],
            "200"
        );
    }
}
