use chrono::{TimeZone, Utc};
use home_db_importer::state_management::{load_import_state, save_import_state, ImportState};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::tempdir;

// Test saving and then loading state
#[test]
fn test_save_load_state() {
    // Create a temporary directory for test files
    let temp_dir = tempdir().unwrap();
    let state_file_path = temp_dir.path().join("test_state.json");
    let state_file = state_file_path.to_str().unwrap();

    // Source file path to include in the state
    let source_file = "test_data.csv";

    // Create an initial state
    let timestamp = Utc.with_ymd_and_hms(2023, 7, 15, 10, 30, 0).unwrap();
    let mut state = ImportState::new(source_file);
    state.last_imported_timestamp = Some(timestamp);
    state.records_imported = 42;

    // Save the state
    let save_result = save_import_state(&state, state_file);
    assert!(save_result.is_ok());

    // Check that the file was created
    assert!(Path::new(state_file).exists());

    // Load the state back and verify it matches
    let loaded_state = load_import_state(state_file, source_file);

    assert_eq!(loaded_state.source_file, source_file);
    assert_eq!(loaded_state.records_imported, 42);
    assert_eq!(loaded_state.last_imported_timestamp, Some(timestamp));
}

// Test loading state with a different source file
#[test]
fn test_load_state_different_source() {
    // Create a temporary directory for test files
    let temp_dir = tempdir().unwrap();
    let state_file_path = temp_dir.path().join("test_state.json");
    let state_file = state_file_path.to_str().unwrap();

    // Source file paths
    let original_source = "original.csv";
    let different_source = "different.csv";

    // Create an initial state
    let timestamp = Utc.with_ymd_and_hms(2023, 7, 15, 10, 30, 0).unwrap();
    let mut state = ImportState::new(original_source);
    state.last_imported_timestamp = Some(timestamp);
    state.records_imported = 42;

    // Save the state
    save_import_state(&state, state_file).unwrap();

    // Load with a different source file - should return a new state
    let loaded_state = load_import_state(state_file, different_source);

    // Should be a new state for the different source
    assert_eq!(loaded_state.source_file, different_source);
    assert_eq!(loaded_state.records_imported, 0);
    assert_eq!(loaded_state.last_imported_timestamp, None);
}

// Test loading from a non-existent file
#[test]
fn test_load_nonexistent_file() {
    let state_file = "nonexistent_state_file.json";
    let source_file = "test.csv";

    // Ensure the file doesn't exist
    if Path::new(state_file).exists() {
        fs::remove_file(state_file).unwrap();
    }

    // Try to load from non-existent file
    let state = load_import_state(state_file, source_file);

    // Should return a new default state
    assert_eq!(state.source_file, source_file);
    assert_eq!(state.records_imported, 0);
    assert_eq!(state.last_imported_timestamp, None);
}

// Test loading from a corrupted file
#[test]
fn test_load_corrupted_file() {
    // Create a temporary directory for test files
    let temp_dir = tempdir().unwrap();
    let state_file_path = temp_dir.path().join("corrupted_state.json");
    let state_file = state_file_path.to_str().unwrap();
    let source_file = "test.csv";

    // Write corrupted JSON to the file
    let mut file = File::create(state_file).unwrap();
    file.write_all(b"{this is not valid json}").unwrap();

    // Try to load from corrupted file
    let state = load_import_state(state_file, source_file);

    // Should return a new default state
    assert_eq!(state.source_file, source_file);
    assert_eq!(state.records_imported, 0);
    assert_eq!(state.last_imported_timestamp, None);
}
