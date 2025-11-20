use crate::database::{KeyTypes, ValueTypes, store::Store};
use tempfile::NamedTempFile;

#[test]
fn test_invalid_store_name() {
    let tmpfile = NamedTempFile::new().unwrap();
    let path = tmpfile.path();

    // Test invalid names
    assert!(Store::create("123db".to_string(), path).is_err());
    assert!(Store::create("my::db".to_string(), path).is_err());
    assert!(Store::create("test__meta".to_string(), path).is_err());
    assert!(Store::create("".to_string(), path).is_err());

    // Test valid name
    assert!(Store::create("valid_db".to_string(), path).is_ok());
}

#[test]
fn test_invalid_collection_name() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Test invalid names
    assert!(
        store
            .collection("1users", KeyTypes::String, ValueTypes::String)
            .is_err()
    );
    assert!(
        store
            .collection("test::users", KeyTypes::String, ValueTypes::String)
            .is_err()
    );
    assert!(
        store
            .collection("my__collection", KeyTypes::String, ValueTypes::String)
            .is_err()
    );

    // Test valid name
    assert!(
        store
            .collection("valid_collection", KeyTypes::String, ValueTypes::String)
            .is_ok()
    );
}
