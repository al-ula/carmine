use crate::store::Store;
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
