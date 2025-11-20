use std::sync::Arc;

use crate::Number;
use crate::store::Store;
use redb::ReadableDatabase;
use tempfile::NamedTempFile;
use crate::collection::{CollectionHandle, ReadOnlyCollectionHandle};
use crate::key::KeyTypes;
use crate::value::ValueTypes;

#[test]
fn test_create_collection() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();
    let write_txn = store.handle.begin_write().unwrap();

    let collection = store
        .collection("test_collection", KeyTypes::String, ValueTypes::String)
        .unwrap();

    {
        let write_handle = collection.write(&write_txn).unwrap();

        if let CollectionHandle::StrStr(mut table) = write_handle {
            table.insert("foo", "bar").unwrap();
        } else {
            panic!("Wrong handle type");
        }
    }

    write_txn.commit().unwrap();

    let read_txn = store.handle.begin_read().unwrap();
    let read_handle = collection.read(&read_txn).unwrap();

    if let ReadOnlyCollectionHandle::StrStr(table) = read_handle {
        let value = table.get("foo").unwrap().unwrap();
        assert_eq!(value.value(), "bar");
    } else {
        panic!("Wrong handle type");
    }
}

#[test]
fn test_create_collection_all_types() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();
    let write_txn = store.handle.begin_write().unwrap();

    let combinations = [
        (KeyTypes::String, ValueTypes::String),
        (KeyTypes::BigInt, ValueTypes::Number),
        (KeyTypes::Number, ValueTypes::Bytes),
        (KeyTypes::Bytes, ValueTypes::Object),
    ];

    for (i, (k, v)) in combinations.iter().enumerate() {
        let collection = store
            .collection(format!("collection_{}", i), *k, *v)
            .unwrap();
        let _ = collection.write(&write_txn).unwrap();
    }

    write_txn.commit().unwrap();

    let read_txn = store.handle.begin_read().unwrap();
    for (i, (k, v)) in combinations.iter().enumerate() {
        let collection = store
            .collection(format!("collection_{}", i), *k, *v)
            .unwrap();
        let _ = collection.read(&read_txn).unwrap();
    }
}

#[test]
fn test_collection_with_number_keys() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    let collection = store
        .collection("numbers", KeyTypes::Number, ValueTypes::String)
        .unwrap();

    let write_txn = store.handle.begin_write().unwrap();
    if let CollectionHandle::NumberStr(mut table) = collection.write(&write_txn).unwrap() {
        table.insert(Number::from(1.5), "one point five").unwrap();
        table.insert(Number::from(2.7), "two point seven").unwrap();
        table.insert(Number::from(-3.14), "negative pi").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = store.handle.begin_read().unwrap();
    if let ReadOnlyCollectionHandle::NumberStr(table) = collection.read(&read_txn).unwrap() {
        assert_eq!(
            table.get(Number::from(1.5)).unwrap().unwrap().value(),
            "one point five"
        );
        assert_eq!(
            table.get(Number::from(2.7)).unwrap().unwrap().value(),
            "two point seven"
        );
        assert_eq!(
            table.get(Number::from(-3.14)).unwrap().unwrap().value(),
            "negative pi"
        );
    }
}

#[test]
fn test_collection_with_bytes() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    let collection = store
        .collection("binary_data", KeyTypes::String, ValueTypes::Bytes)
        .unwrap();

    let data1: &[u8] = b"\x00\x01\x02\x03\x04";
    let data2: &[u8] = b"Hello, World!";

    let write_txn = store.handle.begin_write().unwrap();
    if let CollectionHandle::StrBytes(mut table) = collection.write(&write_txn).unwrap() {
        table.insert("binary", data1).unwrap();
        table.insert("text", data2).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = store.handle.begin_read().unwrap();
    if let ReadOnlyCollectionHandle::StrBytes(table) = collection.read(&read_txn).unwrap() {
        assert_eq!(table.get("binary").unwrap().unwrap().value(), data1);
        assert_eq!(table.get("text").unwrap().unwrap().value(), data2);
    }
}

#[test]
fn test_collection_persistence() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Create store, write data, and close
    {
        let store = Store::create("test_store".to_string(), path).unwrap();
        let collection = store
            .collection("persistent", KeyTypes::String, ValueTypes::BigInt)
            .unwrap();

        let write_txn = store.handle.begin_write().unwrap();
        if let CollectionHandle::StrInt(mut table) = collection.write(&write_txn).unwrap() {
            table.insert("count", 42).unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Reopen store and verify data persisted
    {
        let store = Store::open("test_store".to_string(), path).unwrap();
        let collection = store
            .collection("persistent", KeyTypes::String, ValueTypes::BigInt)
            .unwrap();

        let read_txn = store.handle.begin_read().unwrap();
        if let ReadOnlyCollectionHandle::StrInt(table) = collection.read(&read_txn).unwrap() {
            assert_eq!(table.get("count").unwrap().unwrap().value(), 42);
        }
    }
}

macro_rules! test_kv {
    ($store:expr, $name:expr, $k_type:expr, $v_type:expr, $k:expr, $v:expr, $write_variant:ident, $read_variant:ident) => {{
        let coll = $store.collection($name, $k_type, $v_type).unwrap();
        {
            let write_txn = $store.handle.begin_write().unwrap();
            if let CollectionHandle::$write_variant(mut table) = coll.write(&write_txn).unwrap() {
                table.insert($k, $v).unwrap();
            } else {
                panic!("Wrong write handle");
            }
            write_txn.commit().unwrap();
        }
        {
            let read_txn = $store.handle.begin_read().unwrap();
            if let ReadOnlyCollectionHandle::$read_variant(table) = coll.read(&read_txn).unwrap() {
                let val = table.get($k).unwrap().unwrap();
                assert_eq!(val.value(), $v);
            } else {
                panic!("Wrong read handle");
            }
        }
    }};
}

#[test]
fn test_all_key_value_combinations() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // String Keys
    test_kv!(
        store,
        "str_str",
        KeyTypes::String,
        ValueTypes::String,
        "k1",
        "v1",
        StrStr,
        StrStr
    );
    test_kv!(
        store,
        "str_int",
        KeyTypes::String,
        ValueTypes::BigInt,
        "k1",
        42i64,
        StrInt,
        StrInt
    );
    test_kv!(
        store,
        "str_num",
        KeyTypes::String,
        ValueTypes::Number,
        "k1",
        Number::from(3.14),
        StrNumber,
        StrNumber
    );
    test_kv!(
        store,
        "str_bytes",
        KeyTypes::String,
        ValueTypes::Bytes,
        "k1",
        b"data" as &[u8],
        StrBytes,
        StrBytes
    );

    // BigInt Keys
    test_kv!(
        store,
        "int_str",
        KeyTypes::BigInt,
        ValueTypes::String,
        1i64,
        "v1",
        IntStr,
        IntStr
    );
    test_kv!(
        store,
        "int_int",
        KeyTypes::BigInt,
        ValueTypes::BigInt,
        1i64,
        42i64,
        IntInt,
        IntInt
    );
    test_kv!(
        store,
        "int_num",
        KeyTypes::BigInt,
        ValueTypes::Number,
        1i64,
        Number::from(3.14),
        IntNumber,
        IntNumber
    );
    test_kv!(
        store,
        "int_bytes",
        KeyTypes::BigInt,
        ValueTypes::Bytes,
        1i64,
        b"data" as &[u8],
        IntBytes,
        IntBytes
    );

    // Number Keys
    test_kv!(
        store,
        "num_str",
        KeyTypes::Number,
        ValueTypes::String,
        Number::from(1.5),
        "v1",
        NumberStr,
        NumberStr
    );
    test_kv!(
        store,
        "num_int",
        KeyTypes::Number,
        ValueTypes::BigInt,
        Number::from(1.5),
        42i64,
        NumberInt,
        NumberInt
    );
    test_kv!(
        store,
        "num_num",
        KeyTypes::Number,
        ValueTypes::Number,
        Number::from(1.5),
        Number::from(3.14),
        NumberNumber,
        NumberNumber
    );
    test_kv!(
        store,
        "num_bytes",
        KeyTypes::Number,
        ValueTypes::Bytes,
        Number::from(1.5),
        b"data" as &[u8],
        NumberBytes,
        NumberBytes
    );

    // Bytes Keys
    test_kv!(
        store,
        "bytes_str",
        KeyTypes::Bytes,
        ValueTypes::String,
        b"k1" as &[u8],
        "v1",
        BytesStr,
        BytesStr
    );
    test_kv!(
        store,
        "bytes_int",
        KeyTypes::Bytes,
        ValueTypes::BigInt,
        b"k1" as &[u8],
        42i64,
        BytesInt,
        BytesInt
    );
    test_kv!(
        store,
        "bytes_num",
        KeyTypes::Bytes,
        ValueTypes::Number,
        b"k1" as &[u8],
        Number::from(3.14),
        BytesNumber,
        BytesNumber
    );
    test_kv!(
        store,
        "bytes_bytes",
        KeyTypes::Bytes,
        ValueTypes::Bytes,
        b"k1" as &[u8],
        b"data" as &[u8],
        BytesBytes,
        BytesBytes
    );
}

#[test]
fn test_collection_write_one() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Arc::new(Store::create("test_store".to_string(), tmpfile.path()).unwrap());

    let collection = store
        .collection("write_one_collection", KeyTypes::String, ValueTypes::String)
        .unwrap();

    collection
        .write_one("key1".to_string(), "value1".to_string(), &store)
        .unwrap();

    let read_txn = store.handle.begin_read().unwrap();
    if let ReadOnlyCollectionHandle::StrStr(table) = collection.read(&read_txn).unwrap() {
        let value = table.get("key1").unwrap().unwrap();
        assert_eq!(value.value(), "value1");
    } else {
        panic!("Wrong handle type");
    }
}
