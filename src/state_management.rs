use chrono::{DateTime, Utc};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

/// Structure to hold import state information
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct ImportState {
    pub last_imported_timestamp: Option<DateTime<Utc>>,
    pub source_file: String,
    pub records_imported: usize,
}

impl ImportState {
    pub fn new(source_file: &str) -> Self {
        ImportState {
            last_imported_timestamp: None,
            source_file: source_file.to_string(),
            records_imported: 0,
        }
    }
}

/// Loads the import state from a file
pub fn load_import_state(state_file: &str, source_file: &str) -> ImportState {
    if Path::new(state_file).exists() {
        match File::open(state_file) {
            Ok(mut file) => {
                let mut contents = String::new();
                if file.read_to_string(&mut contents).is_ok() {
                    match serde_json::from_str::<ImportState>(&contents) {
                        Ok(state) => {
                            // Only use the state if it's for the same source file
                            if state.source_file == source_file {
                                return state;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error parsing state file: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error opening state file: {}", e);
            }
        }
    }
    
    // Return a new state if we couldn't load an existing one
    ImportState::new(source_file)
}

/// Saves the import state to a file
pub fn save_import_state(
    state: &ImportState,
    state_file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(state)?;
    let mut file = File::create(state_file)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}
