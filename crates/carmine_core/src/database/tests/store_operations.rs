use crate::database::store::Store;
use redb::ReadableDatabase;
use tempfile::NamedTempFile;

#[test]
fn test_store_create_and_open() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a new store
    {
        let mut store = Store::create("test_store".to_string(), path).unwrap();
        let write_txn = store.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Reopen the store
    let store = Store::open("test_store".to_string(), path).unwrap();
    assert_eq!(store.name, "test_store");

    // Verify we can read from it
    let _read_txn = store.begin_read().unwrap();
}

#[test]
fn test_store_compact() {
    let tmpfile = NamedTempFile::new().unwrap();
    let mut store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Add some data
    {
        let write_txn = store.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Compact should succeed
    store.compact().unwrap();
}

#[test]
fn test_store_check_integrity() {
    let tmpfile = NamedTempFile::new().unwrap();
    let mut store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Add some data
    {
        let write_txn = store.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Integrity check should pass
    store.check_integrity().unwrap();
}

#[test]
fn test_store_begin_write() {
    let tmpfile = NamedTempFile::new().unwrap();
    let mut store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Should be able to create a write transaction
    let write_txn = store.begin_write().unwrap();
    write_txn.commit().unwrap();
}

#[test]
fn test_store_cache_stats() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Should be able to get cache stats
    let stats = store.cache_stats();
    // Just verify we can call it without panicking
    let _ = stats;
}

#[test]
fn test_store_builder() {
    let tmpfile = NamedTempFile::new().unwrap();

    // Create store using builder
    let builder = Store::builder("test_store").unwrap();
    let store = builder.create(tmpfile.path()).unwrap();

    assert_eq!(store.name, "test_store");
}
