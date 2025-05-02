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
}

/// Represents a parsed CSV record
pub struct CsvRecord {
    pub headers: Vec<String>,
    pub values: HashMap<String, String>,
}

impl fmt::Display for CsvRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Record:")?;
        for header in &self.headers {
            if let Some(value) = self.values.get(header) {
                writeln!(f, "  {}: {}", header, value)?;
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
            header_rows: 1, // Default to 1 header row
        }
    }

    /// Sets the number of rows that make up the header
    pub fn with_header_rows(mut self, rows: usize) -> Self {
        self.header_rows = rows;
        self
    }

    /// Gets the number of header rows
    pub fn header_rows(&self) -> usize {
        self.header_rows
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
                column_headers.push(field.replace(' ', "_"));
            }
            return column_headers;
        }

        // If we have multiple header rows, combine them
        let columns = headers[0].len();
        for col in 0..columns {
            let mut parts = Vec::new();

            for row in headers {
                if col < row.len() {
                    parts.push(row[col].to_string());
                }
            }

            let header = parts.join(".").replace(' ', "_");
            column_headers.push(header);
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

        // Create CSV reader
        let mut rdr = ReaderBuilder::new()
            .has_headers(false) // We'll handle headers manually
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
        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

        // Skip header rows
        for _ in 0..self.header_rows {
            if rdr.records().next().is_none() {
                break;
            }
        }

        // Read data rows
        for result in rdr.records() {
            let record = result?;
            let mut values = HashMap::new();

            // Map each field to its header
            for (i, field) in record.iter().enumerate() {
                if i < headers.len() {
                    values.insert(headers[i].clone(), field.to_string());
                }
            }

            records.push(CsvRecord {
                headers: headers.clone(),
                values,
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
            records[0].headers.len()
        ));
        output.push_str("Headers: ");
        output.push_str(&records[0].headers.join(", "));
        output.push_str("\n\nSample data:\n");

        // Show up to 5 records as samples
        let sample_size = std::cmp::min(5, records.len());
        for (i, record) in records.iter().take(sample_size).enumerate() {
            output.push_str(&format!("\nRecord {}:\n", i + 1));
            for header in &record.headers {
                if let Some(value) = record.values.get(header) {
                    output.push_str(&format!("  {}: {}\n", header, value));
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
            match self.format_parsed_data() {
                Ok(formatted) => {
                    output.push_str("\nParsed Data Details:\n");
                    output.push_str(&formatted);
                }
                Err(e) => {
                    output.push_str(&format!("\nError parsing data for details: {}\n", e));
                }
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

    #[test]
    fn test_new_parser() {
        let parser = CsvParser::new("test_file.csv");
        assert_eq!(parser.file_path, "test_file.csv");
        assert_eq!(parser.header_rows(), 1); // Default is 1 header row
    }

    #[test]
    fn test_with_header_rows() {
        let parser = CsvParser::new("test_file.csv").with_header_rows(2);
        assert_eq!(parser.header_rows(), 2);
    }

    #[test]
    fn test_file_exists_nonexistent_file() {
        let parser = CsvParser::new("nonexistent_file.csv");
        assert!(!parser.file_exists());
    }

    #[test]
    fn test_file_exists_real_file() {
        // Create a real temporary file
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let parser = CsvParser::new(path);
        assert!(parser.file_exists());
    }
}
