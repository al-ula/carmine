use crate::store::Store;
use redb::ReadableDatabase;
use tempfile::NamedTempFile;

#[test]
fn test_store_readable_drop() {
    let tmpfile = NamedTempFile::new().unwrap();
    let store = Store::create("test_store".to_string(), tmpfile.path()).unwrap();

    // Verify ReadableDatabase trait implementation
    fn accepts_readable<T: ReadableDatabase>(_: &T) {}
    accepts_readable(&store);

    // Verify begin_read works
    let _read_txn = store.begin_read().unwrap();
}
