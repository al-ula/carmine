use crate::store::{ReadOnlyStore, Store};
use redb::ReadableDatabase;
use tempfile::NamedTempFile;
use crate::collection::ReadOnlyCollectionHandle;
use crate::key::KeyTypes;
use crate::value::ValueTypes;

#[test]
fn test_readonly_store_open() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database first
    {
        let store = Store::create("test_store".to_string(), path).unwrap();
        let write_txn = store.handle.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Open as read-only
    let readonly_store = ReadOnlyStore::open("readonly_store".to_string(), path).unwrap();
    assert_eq!(readonly_store.name, "readonly_store");
}

#[test]
fn test_readonly_store_collection() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database first
    {
        let store = Store::create("test_store".to_string(), path).unwrap();
        let write_txn = store.handle.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Open as read-only and create collection
    let readonly_store = ReadOnlyStore::open("readonly_store".to_string(), path).unwrap();
    let collection = readonly_store
        .collection("test_collection", KeyTypes::String, ValueTypes::String)
        .unwrap();

    assert_eq!(collection.name, "test_collection");
    assert_eq!(collection.key_type, KeyTypes::String);
    assert_eq!(collection.value_type, ValueTypes::String);
}

#[test]
fn test_readonly_store_read_data() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database and write some data
    {
        let store = Store::create("test_store".to_string(), path).unwrap();
        let collection = store
            .collection("test_collection", KeyTypes::String, ValueTypes::String)
            .unwrap();

        let write_txn = store.handle.begin_write().unwrap();

        {
            let write_handle = collection.write(&write_txn).unwrap();

            if let crate::collection::CollectionHandle::StrStr(mut table) = write_handle {
                table.insert("key1", "value1").unwrap();
                table.insert("key2", "value2").unwrap();
            }
        } // write_handle is dropped here

        write_txn.commit().unwrap();
    }

    // Open as read-only and read data
    let readonly_store = ReadOnlyStore::open("readonly_store".to_string(), path).unwrap();
    let collection = readonly_store
        .collection("test_collection", KeyTypes::String, ValueTypes::String)
        .unwrap();

    let read_txn = readonly_store.begin_read().unwrap();
    let read_handle = collection.read(&read_txn).unwrap();

    if let ReadOnlyCollectionHandle::StrStr(table) = read_handle {
        let value1 = table.get("key1").unwrap().unwrap();
        assert_eq!(value1.value(), "value1");

        let value2 = table.get("key2").unwrap().unwrap();
        assert_eq!(value2.value(), "value2");
    } else {
        panic!("Wrong handle type");
    }
}

#[test]
fn test_readonly_store_readable_trait() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create a database first
    {
        let store = Store::create("test_store".to_string(), path).unwrap();
        let write_txn = store.handle.begin_write().unwrap();
        write_txn.commit().unwrap();
    }

    // Open as read-only
    let readonly_store = ReadOnlyStore::open("readonly_store".to_string(), path).unwrap();

    // Verify ReadableDatabase trait implementation
    fn accepts_readable<T: ReadableDatabase>(_: &T) {}
    accepts_readable(&readonly_store);

    // Verify begin_read works
    let _read_txn = readonly_store.begin_read().unwrap();

    // Verify cache_stats works
    let _stats = readonly_store.cache_stats();
}
