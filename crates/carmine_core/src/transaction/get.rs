use crate::error::Error;
use redb::{ReadableDatabase, TableDefinition};

use super::Transaction;

use crate::{
    key::{Key, KeyType},
    types::{Int, Number, RawObject},
    value::{BatchItemError, Value, ValueRetVec, ValueType},
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
            .map_err(|e| Error::Shelf(e.into()))?;
        let search_key = $key_conv($key.clone());
        let value = table_handle
            .get(search_key)
            .map_err(|e| Error::Transaction(e.into()))?;
        Ok(value.map(|v| $val_wrap(v.value())))
    }};
}

/// Helper macro to perform a batch typed table lookup in redb.
/// Takes an errors array and sets Err for mismatched keys, Ok for found/not found values.
macro_rules! batch_get_typed {
    ($read_txn:expr, $bucket_name:expr, $keys:expr, $errors:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_conv:expr, $RetVariant:ident) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($bucket_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(|e| Error::Shelf(e.into()))?;

        let mut result_vec: Vec<std::result::Result<Option<_>, BatchItemError>> =
            Vec::with_capacity($keys.len());

        for (i, key) in $keys.iter().enumerate() {
            if let Some(err) = &$errors[i] {
                result_vec.push(Err(err.clone()));
            } else if let Ok(search_key) = $key_conv(key.clone()) {
                match table_handle.get(search_key) {
                    Ok(Some(value)) => {
                        result_vec.push(Ok(Some($val_conv(value.value()))));
                    }
                    Ok(None) => {
                        result_vec.push(Ok(None));
                    }
                    Err(_e) => {
                        result_vec.push(Ok(None));
                    }
                }
            } else {
                result_vec.push(Ok(None));
            }
        }
        Ok(ValueRetVec::$RetVariant(result_vec))
    }};
}

pub trait GetTxn<'a> {
    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn batch_get(&self, keys: &[Key]) -> Result<ValueRetVec>;
}

impl<'a> GetTxn<'a> for Transaction<'a> {
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.validate_key_type(key).map_err(Error::Transaction)?;

        let database = self.cabinet.open().map_err(|e| Error::Cabinet(e.into()))?;
        let read_txn = database
            .begin_read()
            .map_err(|e| Error::Transaction(e.into()))?;

        let name = &self.shelf.name;

        let key_to_string = |k: Key| -> String {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a String key"))
        };
        let key_to_number = |k: Key| -> Number {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a Number key"))
        };
        let key_to_int = |k: Key| -> i64 {
            let i: Int = k
                .try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees an Int key"));
            *i
        };

        match (self.shelf.key_type, self.shelf.value_type) {
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

    fn batch_get(&self, keys: &[Key]) -> Result<ValueRetVec> {
        let bucket_key_type = self.shelf.key_type;

        let errors: Vec<Option<BatchItemError>> = keys
            .iter()
            .map(|k| {
                if k.as_type() != bucket_key_type {
                    Some(BatchItemError::TypeMismatch {
                        expected: format!("{:?}", bucket_key_type),
                        actual: format!("{:?}", k.as_type()),
                    })
                } else {
                    None
                }
            })
            .collect();

        let database = self.cabinet.open().map_err(|e| Error::Cabinet(e.into()))?;
        let read_txn = database
            .begin_read()
            .map_err(|e| Error::Transaction(e.into()))?;

        let name = &self.shelf.name;

        // Use safe conversion closures that return Results so we can cleanly fail on bad types
        // without panicking, leaving the result vector slot as `None`
        let safe_key_to_string =
            |k: Key| -> std::result::Result<String, crate::key::KeyError> { k.try_into() };
        let safe_key_to_number =
            |k: Key| -> std::result::Result<Number, crate::key::KeyError> { k.try_into() };
        let safe_key_to_int = |k: Key| -> std::result::Result<i64, crate::key::KeyError> {
            let i: std::result::Result<Int, crate::key::KeyError> = k.try_into();
            i.map(|v| *v)
        };

        match (self.shelf.key_type, self.shelf.value_type) {
            // String keys
            (KeyType::String, ValueType::String) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    String,
                    safe_key_to_string,
                    String,
                    |v: String| v,
                    String
                )
            }
            (KeyType::String, ValueType::Number) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    String,
                    safe_key_to_string,
                    Number,
                    |v: Number| v,
                    Number
                )
            }
            (KeyType::String, ValueType::Int) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    String,
                    safe_key_to_string,
                    Int,
                    |v: i64| Int(v),
                    Int
                )
            }
            (KeyType::String, ValueType::Object) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    String,
                    safe_key_to_string,
                    RawObject,
                    |v: RawObject| v,
                    Object
                )
            }
            (KeyType::String, ValueType::Byte) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    String,
                    safe_key_to_string,
                    &[u8],
                    |v: &[u8]| v.to_vec(),
                    Byte
                )
            }

            // Number keys
            (KeyType::Number, ValueType::String) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Number,
                    safe_key_to_number,
                    String,
                    |v: String| v,
                    String
                )
            }
            (KeyType::Number, ValueType::Number) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Number,
                    safe_key_to_number,
                    Number,
                    |v: Number| v,
                    Number
                )
            }
            (KeyType::Number, ValueType::Int) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Number,
                    safe_key_to_number,
                    Int,
                    |v: i64| Int(v),
                    Int
                )
            }
            (KeyType::Number, ValueType::Object) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Number,
                    safe_key_to_number,
                    RawObject,
                    |v: RawObject| v,
                    Object
                )
            }
            (KeyType::Number, ValueType::Byte) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Number,
                    safe_key_to_number,
                    &[u8],
                    |v: &[u8]| v.to_vec(),
                    Byte
                )
            }

            // Int keys
            (KeyType::Int, ValueType::String) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Int,
                    safe_key_to_int,
                    String,
                    |v: String| v,
                    String
                )
            }
            (KeyType::Int, ValueType::Number) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Int,
                    safe_key_to_int,
                    Number,
                    |v: Number| v,
                    Number
                )
            }
            (KeyType::Int, ValueType::Int) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Int,
                    safe_key_to_int,
                    Int,
                    |v: i64| Int(v),
                    Int
                )
            }
            (KeyType::Int, ValueType::Object) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Int,
                    safe_key_to_int,
                    RawObject,
                    |v: RawObject| v,
                    Object
                )
            }
            (KeyType::Int, ValueType::Byte) => {
                batch_get_typed!(
                    read_txn,
                    name,
                    keys,
                    errors,
                    Int,
                    safe_key_to_int,
                    &[u8],
                    |v: &[u8]| v.to_vec(),
                    Byte
                )
            }
        }
    }
}
