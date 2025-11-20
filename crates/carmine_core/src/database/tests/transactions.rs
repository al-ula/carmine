use crate::Number;
use crate::database::store::Store;
use crate::database::{KeyTypes, ReadOnlyTableHandle, TableHandle, ValueTypes};
use redb::ReadableDatabase;
use tempfile::NamedTempFile;

#[test]
fn test_multiple_collections_same_transaction() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    let coll1 = store
        .collection("collection1", KeyTypes::String, ValueTypes::String)
        .unwrap();
    let coll2 = store
        .collection("collection2", KeyTypes::BigInt, ValueTypes::BigInt)
        .unwrap();
    let coll3 = store
        .collection("collection3", KeyTypes::Number, ValueTypes::Number)
        .unwrap();

    // Write to all collections in one transaction
    let write_txn = store.handle.begin_write().unwrap();

    if let TableHandle::StrStr(mut table) = coll1.write(&write_txn).unwrap() {
        table.insert("key1", "value1").unwrap();
    }

    if let TableHandle::IntInt(mut table) = coll2.write(&write_txn).unwrap() {
        table.insert(42, 100).unwrap();
    }

    if let TableHandle::NumberNumber(mut table) = coll3.write(&write_txn).unwrap() {
        table
            .insert(Number::from(3.14), Number::from(2.71))
            .unwrap();
    }

    write_txn.commit().unwrap();

    // Read from all collections
    let read_txn = store.handle.begin_read().unwrap();

    if let ReadOnlyTableHandle::StrStr(table) = coll1.read(&read_txn).unwrap() {
        assert_eq!(table.get("key1").unwrap().unwrap().value(), "value1");
    }

    if let ReadOnlyTableHandle::IntInt(table) = coll2.read(&read_txn).unwrap() {
        assert_eq!(table.get(42).unwrap().unwrap().value(), 100);
    }

    if let ReadOnlyTableHandle::NumberNumber(table) = coll3.read(&read_txn).unwrap() {
        assert_eq!(
            table.get(Number::from(3.14)).unwrap().unwrap().value(),
            Number::from(2.71)
        );
    }
}

#[test]
fn test_read_write_isolation() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();
    let collection = store
        .collection("isolation_test", KeyTypes::String, ValueTypes::String)
        .unwrap();

    // Write initial data
    {
        let write_txn = store.handle.begin_write().unwrap();
        if let TableHandle::StrStr(mut table) = collection.write(&write_txn).unwrap() {
            table.insert("key1", "initial").unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Start a read transaction
    let read_txn = store.handle.begin_read().unwrap();

    // Modify data in a write transaction
    {
        let write_txn = store.handle.begin_write().unwrap();
        if let TableHandle::StrStr(mut table) = collection.write(&write_txn).unwrap() {
            table.insert("key1", "modified").unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Read transaction should still see old data (snapshot isolation)
    if let ReadOnlyTableHandle::StrStr(table) = collection.read(&read_txn).unwrap() {
        assert_eq!(table.get("key1").unwrap().unwrap().value(), "initial");
    }

    // New read transaction should see new data
    let new_read_txn = store.handle.begin_read().unwrap();
    if let ReadOnlyTableHandle::StrStr(table) = collection.read(&new_read_txn).unwrap() {
        assert_eq!(table.get("key1").unwrap().unwrap().value(), "modified");
    }
}
