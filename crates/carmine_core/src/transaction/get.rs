use crate::error::Error;
use redb::{ReadableDatabase, TableDefinition};

use super::Transaction;

use crate::{
    key::{Key, KeyType},
    types::{Int, Number, RawObject},
    value::{Value, ValueType},
};

type Result<T> = std::result::Result<T, Error>;

/// Helper macro to perform a typed table lookup in redb.
///
/// # Arguments
/// - `$read_txn`: the redb read transaction
/// - `$bucket_name`: expression yielding `&str` for the table name
/// - `$key`: the `Key` enum value to look up
/// - `$KeyRedb`: the redb key type (e.g. `String`, `Number`, `Int`)
/// - `$key_conv`: closure/fn to convert a cloned `Key` into the redb-native key type
/// - `$ValRedb`: the redb value type (e.g. `String`, `Number`, `Int`, `RawObject`, `&[u8]`)
/// - `$val_wrap`: closure that converts the redb `AccessGuard::value()` into a `Value`
macro_rules! get_typed {
    ($read_txn:expr, $bucket_name:expr, $key:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_wrap:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($bucket_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(|e| Error::Bucket(e.into()))?;
        let search_key = $key_conv($key.clone());
        let value = table_handle
            .get(search_key)
            .map_err(|e| Error::Transaction(e.into()))?;
        Ok(value.map(|v| $val_wrap(v.value())))
    }};
}

pub trait GetTxn<'a> {
    fn get(&self, key: &Key) -> Result<Option<Value>>;
}

impl<'a> GetTxn<'a> for Transaction<'a> {
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        let database = self.tub.open().map_err(|e| Error::Tub(e.into()))?;
        let read_txn = database
            .begin_read()
            .map_err(|e| Error::Transaction(e.into()))?;

        let name = &self.bucket.name;

        let key_to_string = |k: Key| -> String {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Bucket key_type guarantees a String key"))
        };
        let key_to_number = |k: Key| -> Number {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Bucket key_type guarantees a Number key"))
        };
        let key_to_int = |k: Key| -> i64 {
            let i: Int = k
                .try_into()
                .unwrap_or_else(|_| unreachable!("Bucket key_type guarantees an Int key"));
            *i
        };

        match (self.bucket.key_type, self.bucket.value_type) {
            // String keys
            (KeyType::String, ValueType::String) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    String,
                    key_to_string,
                    String,
                    |v: String| Value::String(v)
                )
            }
            (KeyType::String, ValueType::Number) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    String,
                    key_to_string,
                    Number,
                    |v: Number| Value::Number(v)
                )
            }
            (KeyType::String, ValueType::Int) => {
                get_typed!(read_txn, name, key, String, key_to_string, Int, |v: i64| {
                    Value::Int(Int(v))
                })
            }
            (KeyType::String, ValueType::Object) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    String,
                    key_to_string,
                    RawObject,
                    |v: RawObject| Value::Object(v)
                )
            }
            (KeyType::String, ValueType::Byte) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    String,
                    key_to_string,
                    &[u8],
                    |v: &[u8]| Value::Byte(v.to_vec())
                )
            }

            // Number keys
            (KeyType::Number, ValueType::String) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    Number,
                    key_to_number,
                    String,
                    |v: String| Value::String(v)
                )
            }
            (KeyType::Number, ValueType::Number) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    Number,
                    key_to_number,
                    Number,
                    |v: Number| Value::Number(v)
                )
            }
            (KeyType::Number, ValueType::Int) => {
                get_typed!(read_txn, name, key, Number, key_to_number, Int, |v: i64| {
                    Value::Int(Int(v))
                })
            }
            (KeyType::Number, ValueType::Object) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    Number,
                    key_to_number,
                    RawObject,
                    |v: RawObject| Value::Object(v)
                )
            }
            (KeyType::Number, ValueType::Byte) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    Number,
                    key_to_number,
                    &[u8],
                    |v: &[u8]| Value::Byte(v.to_vec())
                )
            }

            // Int keys
            (KeyType::Int, ValueType::String) => {
                get_typed!(read_txn, name, key, Int, key_to_int, String, |v: String| {
                    Value::String(v)
                })
            }
            (KeyType::Int, ValueType::Number) => {
                get_typed!(read_txn, name, key, Int, key_to_int, Number, |v: Number| {
                    Value::Number(v)
                })
            }
            (KeyType::Int, ValueType::Int) => {
                get_typed!(read_txn, name, key, Int, key_to_int, Int, |v: i64| {
                    Value::Int(Int(v))
                })
            }
            (KeyType::Int, ValueType::Object) => {
                get_typed!(
                    read_txn,
                    name,
                    key,
                    Int,
                    key_to_int,
                    RawObject,
                    |v: RawObject| Value::Object(v)
                )
            }
            (KeyType::Int, ValueType::Byte) => {
                get_typed!(read_txn, name, key, Int, key_to_int, &[u8], |v: &[u8]| {
                    Value::Byte(v.to_vec())
                })
            }
        }
    }
}
