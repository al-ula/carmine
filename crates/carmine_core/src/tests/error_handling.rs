use crate::store::Store;
use redb::ReadableDatabase;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use crate::collection::ReadOnlyCollectionHandle;
use crate::key::KeyTypes;
use crate::value::ValueTypes;

#[test]
fn test_open_nonexistent_database() {
    // Try to open a database that doesn't exist
    let nonexistent_path = PathBuf::from("/tmp/nonexistent_db_12345.redb");

    let result = Store::open("test_store".to_string(), &nonexistent_path);

    // Should return an error
    assert!(result.is_err());
}

#[test]
fn test_read_nonexistent_table() {
    use crate::database::{};
    use redb::ReadableDatabase;

    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Create a collection but don't write to it
    let collection = store
        .collection("nonexistent", KeyTypes::String, ValueTypes::String)
        .unwrap();

    // Try to read from a table that was never created
    let read_txn = store.begin_read().unwrap();
    let result = collection.read(&read_txn);

    // This should fail because the table doesn't exist
    assert!(result.is_err());
}

#[test]
fn test_invalid_path() {
    // Try to create a database with an invalid path
    let invalid_path = PathBuf::from("/invalid/path/that/does/not/exist/db.redb");

    let result = Store::create("test_store".to_string(), &invalid_path);

    // Should return an error
    assert!(result.is_err());
}

#[test]
fn test_collection_read_empty_table() {
    use crate::database::{};
    use redb::ReadableDatabase;

    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    let collection = store
        .collection("empty", KeyTypes::String, ValueTypes::String)
        .unwrap();

    // Create the table but don't insert anything
    {
        let write_txn = store.handle.begin_write().unwrap();
        let _ = collection.write(&write_txn).unwrap();
        write_txn.commit().unwrap();
    }

    // Read from empty table
    let read_txn = store.begin_read().unwrap();
    if let ReadOnlyCollectionHandle::StrStr(table) = collection.read(&read_txn).unwrap() {
        // Getting a non-existent key should return None
        let result = table.get("nonexistent").unwrap();
        assert!(result.is_none());
    }
}

#[test]
fn test_readonly_store_cannot_write() {
    use crate::store::ReadOnlyStore;

    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database first
    {
        let store = Store::create("test_store".to_string(), path).unwrap();
        let write_txn = store.handle.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Open as read-only
    let readonly_store = ReadOnlyStore::open("readonly".to_string(), path).unwrap();

    // ReadOnlyStore doesn't have begin_write method - this is enforced at compile time
    // This test just verifies we can open it successfully
    let _read_txn = readonly_store.handle.begin_read().unwrap();
}
