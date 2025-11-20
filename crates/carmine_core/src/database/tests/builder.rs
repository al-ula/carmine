use crate::database::store::Store;
use redb::ReadableDatabase;
use std::fs::File;
use tempfile::NamedTempFile;

#[test]
fn test_builder_create() {
    let tmpfile = NamedTempFile::new().unwrap();

    let builder = Store::builder("test_store").unwrap();
    let store = builder.create(tmpfile.path()).unwrap();

    assert_eq!(store.name, "test_store");

    // Verify we can use the store
    let _read_txn = store.begin_read().unwrap();
}

#[test]
fn test_builder_open() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database first
    {
        let builder = Store::builder("test_store").unwrap();
        let store = builder.create(path).unwrap();
        let write_txn = store.handle.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Open it with builder
    let builder = Store::builder("reopened_store").unwrap();
    let store = builder.open(path).unwrap();

    assert_eq!(store.name, "reopened_store");
    let _read_txn = store.begin_read().unwrap();
}

#[test]
fn test_builder_open_read_only() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database first
    {
        let builder = Store::builder("test_store").unwrap();
        let store = builder.create(path).unwrap();
        let write_txn = store.handle.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Open as read-only with builder
    let builder = Store::builder("readonly_store").unwrap();
    let readonly_store = builder.open_read_only(path).unwrap();

    assert_eq!(readonly_store.name, "readonly_store");
    let _read_txn = readonly_store.begin_read().unwrap();
}

#[test]
fn test_builder_set_cache_size() {
    let tmpfile = NamedTempFile::new().unwrap();

    let mut builder = Store::builder("test_store").unwrap();
    builder.set_cache_size(1024 * 1024); // 1MB cache

    let store = builder.create(tmpfile.path()).unwrap();
    assert_eq!(store.name, "test_store");

    // Verify we can use the store
    let _read_txn = store.begin_read().unwrap();
}

#[test]
fn test_builder_create_file() {
    let tmpfile = NamedTempFile::new().unwrap();
    let file = File::options()
        .read(true)
        .write(true)
        .open(tmpfile.path())
        .unwrap();

    let builder = Store::builder("test_store").unwrap();
    let store = builder.create_file(file).unwrap();

    assert_eq!(store.name, "test_store");
    let _read_txn = store.begin_read().unwrap();
}

#[test]
fn test_builder_create_with_backend() {
    // Use in-memory backend for testing
    let backend = redb::backends::InMemoryBackend::new();

    let builder = Store::builder("test_store").unwrap();
    let store = builder.create_with_backend(backend).unwrap();

    assert_eq!(store.name, "test_store");

    // Verify we can use the store
    let _read_txn = store.begin_read().unwrap();
}

#[test]
fn test_builder_chaining() {
    let tmpfile = NamedTempFile::new().unwrap();

    let mut builder = Store::builder("test_store").unwrap();
    let store = builder
        .set_cache_size(512 * 1024) // 512KB cache
        .create(tmpfile.path())
        .unwrap();

    assert_eq!(store.name, "test_store");
    let _read_txn = store.begin_read().unwrap();
}
