use super::Shelf;
use crate::key::{Key, KeyType};
use crate::transaction::{Readable, TransactionError};
use crate::value::{BatchItemError, Value, ValueRetVec, ValueType};
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};

macro_rules! get_typed {
    ($read_txn:expr, $shelf_name:expr, $key:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_wrap:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let search_key = $key_conv($key.clone());
        let value = table_handle
            .get(search_key)
            .map_err(TransactionError::from)?;
        Ok(value.map(|v| $val_wrap(v.value())))
    }};
}

macro_rules! batch_get_typed {
    ($read_txn:expr, $shelf_name:expr, $keys:expr, $errors:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_conv:expr, $RetVariant:ident) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;

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

macro_rules! iter_typed {
    ($read_txn:expr, $shelf_name:expr,
     $KeyRedb:ty, $key_wrap:expr,
     $ValRedb:ty, $val_wrap:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut result = Vec::new();
        let iter = table_handle.iter().map_err(TransactionError::from)?;
        for entry in iter {
            let (key, value) = entry.map_err(TransactionError::from)?;
            result.push(($key_wrap(key.value()), $val_wrap(value.value())));
        }
        Ok(result)
    }};
}

macro_rules! keys_typed {
    ($read_txn:expr, $shelf_name:expr,
     $KeyRedb:ty, $key_wrap:expr,
     $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut result = Vec::new();
        let iter = table_handle.iter().map_err(TransactionError::from)?;
        for entry in iter {
            let (key, _value) = entry.map_err(TransactionError::from)?;
            result.push($key_wrap(key.value()));
        }
        Ok(result)
    }};
}

macro_rules! values_typed {
    ($read_txn:expr, $shelf_name:expr,
     $KeyRedb:ty,
     $ValRedb:ty, $val_wrap:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut result = Vec::new();
        let iter = table_handle.iter().map_err(TransactionError::from)?;
        for entry in iter {
            let (_key, value) = entry.map_err(TransactionError::from)?;
            result.push($val_wrap(value.value()));
        }
        Ok(result)
    }};
}

macro_rules! exists_typed {
    ($read_txn:expr, $shelf_name:expr, $key:expr,
     $KeyRedb:ty, $key_conv:expr, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let search_key = $key_conv($key.clone());
        Ok(table_handle
            .get(search_key)
            .map_err(TransactionError::from)?
            .is_some())
    }};
}

macro_rules! count_typed {
    ($read_txn:expr, $shelf_name:expr, $KeyRedb:ty, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        table_handle.len().map_err(TransactionError::from)
    }};
}

macro_rules! range_typed {
    ($read_txn:expr, $shelf_name:expr, $start:expr, $end:expr,
     $KeyRedb:ty, $key_wrap:expr,
     $ValRedb:ty, $val_wrap:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let table_handle = $read_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let range = $start..$end;
        let mut result = Vec::new();
        let iter = table_handle.range(range).map_err(TransactionError::from)?;
        for entry in iter {
            let (key, value) = entry.map_err(TransactionError::from)?;
            result.push(($key_wrap(key.value()), $val_wrap(value.value())));
        }
        Ok(result)
    }};
}

impl Readable for Shelf {
    fn get(
        &self,
        tx: &redb::ReadTransaction,
        key: &Key,
    ) -> Result<Option<Value>, TransactionError> {
        let key_to_string = |k: Key| -> String {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a String key"))
        };
        let key_to_number = |k: Key| -> crate::types::Number {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a Number key"))
        };
        let key_to_int = |k: Key| -> i64 {
            let i: crate::types::Int = k
                .try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees an Int key"));
            *i
        };

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => get_typed!(
                tx,
                &self.name,
                key,
                String,
                key_to_string,
                String,
                |v: String| Value::String(v)
            ),
            (KeyType::String, ValueType::Number) => get_typed!(
                tx,
                &self.name,
                key,
                String,
                key_to_string,
                crate::types::Number,
                |v: crate::types::Number| Value::Number(v)
            ),
            (KeyType::String, ValueType::Int) => {
                get_typed!(tx, &self.name, key, String, key_to_string, i64, |v: i64| {
                    Value::Int(crate::types::Int(v))
                })
            }
            (KeyType::String, ValueType::Object) => get_typed!(
                tx,
                &self.name,
                key,
                String,
                key_to_string,
                crate::types::RawObject,
                |v: crate::types::RawObject| Value::Object(v)
            ),
            (KeyType::String, ValueType::Byte) => get_typed!(
                tx,
                &self.name,
                key,
                String,
                key_to_string,
                &[u8],
                |v: &[u8]| Value::Byte(v.to_vec())
            ),
            (KeyType::Number, ValueType::String) => get_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                String,
                |v: String| Value::String(v)
            ),
            (KeyType::Number, ValueType::Number) => get_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                crate::types::Number,
                |v: crate::types::Number| Value::Number(v)
            ),
            (KeyType::Number, ValueType::Int) => get_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                i64,
                |v: i64| Value::Int(crate::types::Int(v))
            ),
            (KeyType::Number, ValueType::Object) => get_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                crate::types::RawObject,
                |v: crate::types::RawObject| Value::Object(v)
            ),
            (KeyType::Number, ValueType::Byte) => get_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                &[u8],
                |v: &[u8]| Value::Byte(v.to_vec())
            ),
            (KeyType::Int, ValueType::String) => {
                get_typed!(tx, &self.name, key, i64, key_to_int, String, |v: String| {
                    Value::String(v)
                })
            }
            (KeyType::Int, ValueType::Number) => get_typed!(
                tx,
                &self.name,
                key,
                i64,
                key_to_int,
                crate::types::Number,
                |v: crate::types::Number| Value::Number(v)
            ),
            (KeyType::Int, ValueType::Int) => {
                get_typed!(tx, &self.name, key, i64, key_to_int, i64, |v: i64| {
                    Value::Int(crate::types::Int(v))
                })
            }
            (KeyType::Int, ValueType::Object) => get_typed!(
                tx,
                &self.name,
                key,
                i64,
                key_to_int,
                crate::types::RawObject,
                |v: crate::types::RawObject| Value::Object(v)
            ),
            (KeyType::Int, ValueType::Byte) => {
                get_typed!(tx, &self.name, key, i64, key_to_int, &[u8], |v: &[u8]| {
                    Value::Byte(v.to_vec())
                })
            }
        }
    }

    fn get_batch(
        &self,
        tx: &redb::ReadTransaction,
        keys: &[Key],
    ) -> Result<ValueRetVec, TransactionError> {
        let errors: Vec<Option<BatchItemError>> = keys
            .iter()
            .map(|k| {
                if k.as_type() != self.key_type {
                    Some(BatchItemError::TypeMismatch {
                        expected: format!("{:?}", self.key_type),
                        actual: format!("{:?}", k.as_type()),
                    })
                } else {
                    None
                }
            })
            .collect();

        let safe_key_to_string =
            |k: Key| -> std::result::Result<String, crate::key::KeyError> { k.try_into() };
        let safe_key_to_number = |k: Key| -> std::result::Result<
            crate::types::Number,
            crate::key::KeyError,
        > { k.try_into() };
        let safe_key_to_int = |k: Key| -> std::result::Result<i64, crate::key::KeyError> {
            let i: std::result::Result<crate::types::Int, crate::key::KeyError> = k.try_into();
            i.map(|v| *v)
        };

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                String,
                safe_key_to_string,
                String,
                |v: String| v,
                String
            ),
            (KeyType::String, ValueType::Number) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                String,
                safe_key_to_string,
                crate::types::Number,
                |v: crate::types::Number| v,
                Number
            ),
            (KeyType::String, ValueType::Int) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                String,
                safe_key_to_string,
                i64,
                |v: i64| crate::types::Int(v),
                Int
            ),
            (KeyType::String, ValueType::Object) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                String,
                safe_key_to_string,
                crate::types::RawObject,
                |v: crate::types::RawObject| v,
                Object
            ),
            (KeyType::String, ValueType::Byte) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                String,
                safe_key_to_string,
                &[u8],
                |v: &[u8]| v.to_vec(),
                Byte
            ),
            (KeyType::Number, ValueType::String) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                crate::types::Number,
                safe_key_to_number,
                String,
                |v: String| v,
                String
            ),
            (KeyType::Number, ValueType::Number) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                crate::types::Number,
                safe_key_to_number,
                crate::types::Number,
                |v: crate::types::Number| v,
                Number
            ),
            (KeyType::Number, ValueType::Int) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                crate::types::Number,
                safe_key_to_number,
                i64,
                |v: i64| crate::types::Int(v),
                Int
            ),
            (KeyType::Number, ValueType::Object) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                crate::types::Number,
                safe_key_to_number,
                crate::types::RawObject,
                |v: crate::types::RawObject| v,
                Object
            ),
            (KeyType::Number, ValueType::Byte) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                crate::types::Number,
                safe_key_to_number,
                &[u8],
                |v: &[u8]| v.to_vec(),
                Byte
            ),
            (KeyType::Int, ValueType::String) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                i64,
                safe_key_to_int,
                String,
                |v: String| v,
                String
            ),
            (KeyType::Int, ValueType::Number) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                i64,
                safe_key_to_int,
                crate::types::Number,
                |v: crate::types::Number| v,
                Number
            ),
            (KeyType::Int, ValueType::Int) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                i64,
                safe_key_to_int,
                i64,
                |v: i64| crate::types::Int(v),
                Int
            ),
            (KeyType::Int, ValueType::Object) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                i64,
                safe_key_to_int,
                crate::types::RawObject,
                |v: crate::types::RawObject| v,
                Object
            ),
            (KeyType::Int, ValueType::Byte) => batch_get_typed!(
                tx,
                &self.name,
                keys,
                errors,
                i64,
                safe_key_to_int,
                &[u8],
                |v: &[u8]| v.to_vec(),
                Byte
            ),
        }
    }

    fn exists(&self, tx: &redb::ReadTransaction, key: &Key) -> Result<bool, TransactionError> {
        let key_to_string = |k: Key| -> String {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a String key"))
        };
        let key_to_number = |k: Key| -> crate::types::Number {
            k.try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a Number key"))
        };
        let key_to_int = |k: Key| -> i64 {
            let i: crate::types::Int = k
                .try_into()
                .unwrap_or_else(|_| unreachable!("Validated key_type guarantees an Int key"));
            *i
        };

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => {
                exists_typed!(tx, &self.name, key, String, key_to_string, String)
            }
            (KeyType::String, ValueType::Number) => exists_typed!(
                tx,
                &self.name,
                key,
                String,
                key_to_string,
                crate::types::Number
            ),
            (KeyType::String, ValueType::Int) => {
                exists_typed!(tx, &self.name, key, String, key_to_string, i64)
            }
            (KeyType::String, ValueType::Object) => exists_typed!(
                tx,
                &self.name,
                key,
                String,
                key_to_string,
                crate::types::RawObject
            ),
            (KeyType::String, ValueType::Byte) => {
                exists_typed!(tx, &self.name, key, String, key_to_string, &[u8])
            }
            (KeyType::Number, ValueType::String) => exists_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                String
            ),
            (KeyType::Number, ValueType::Number) => exists_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                crate::types::Number
            ),
            (KeyType::Number, ValueType::Int) => exists_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                i64
            ),
            (KeyType::Number, ValueType::Object) => exists_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => exists_typed!(
                tx,
                &self.name,
                key,
                crate::types::Number,
                key_to_number,
                &[u8]
            ),
            (KeyType::Int, ValueType::String) => {
                exists_typed!(tx, &self.name, key, i64, key_to_int, String)
            }
            (KeyType::Int, ValueType::Number) => {
                exists_typed!(tx, &self.name, key, i64, key_to_int, crate::types::Number)
            }
            (KeyType::Int, ValueType::Int) => {
                exists_typed!(tx, &self.name, key, i64, key_to_int, i64)
            }
            (KeyType::Int, ValueType::Object) => exists_typed!(
                tx,
                &self.name,
                key,
                i64,
                key_to_int,
                crate::types::RawObject
            ),
            (KeyType::Int, ValueType::Byte) => {
                exists_typed!(tx, &self.name, key, i64, key_to_int, &[u8])
            }
        }
    }

    fn count(&self, tx: &redb::ReadTransaction) -> Result<u64, TransactionError> {
        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => count_typed!(tx, &self.name, String, String),
            (KeyType::String, ValueType::Number) => {
                count_typed!(tx, &self.name, String, crate::types::Number)
            }
            (KeyType::String, ValueType::Int) => count_typed!(tx, &self.name, String, i64),
            (KeyType::String, ValueType::Object) => {
                count_typed!(tx, &self.name, String, crate::types::RawObject)
            }
            (KeyType::String, ValueType::Byte) => count_typed!(tx, &self.name, String, &[u8]),
            (KeyType::Number, ValueType::String) => {
                count_typed!(tx, &self.name, crate::types::Number, String)
            }
            (KeyType::Number, ValueType::Number) => {
                count_typed!(tx, &self.name, crate::types::Number, crate::types::Number)
            }
            (KeyType::Number, ValueType::Int) => {
                count_typed!(tx, &self.name, crate::types::Number, i64)
            }
            (KeyType::Number, ValueType::Object) => count_typed!(
                tx,
                &self.name,
                crate::types::Number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => {
                count_typed!(tx, &self.name, crate::types::Number, &[u8])
            }
            (KeyType::Int, ValueType::String) => count_typed!(tx, &self.name, i64, String),
            (KeyType::Int, ValueType::Number) => {
                count_typed!(tx, &self.name, i64, crate::types::Number)
            }
            (KeyType::Int, ValueType::Int) => count_typed!(tx, &self.name, i64, i64),
            (KeyType::Int, ValueType::Object) => {
                count_typed!(tx, &self.name, i64, crate::types::RawObject)
            }
            (KeyType::Int, ValueType::Byte) => count_typed!(tx, &self.name, i64, &[u8]),
        }
    }

    fn get_all(&self, tx: &redb::ReadTransaction) -> Result<Vec<(Key, Value)>, TransactionError> {
        let key_wrap_string = |s: String| Key::String(s);
        let key_wrap_number = |n: crate::types::Number| Key::Number(n);
        let key_wrap_int = |i: i64| Key::Int(crate::types::Int(i));
        let val_wrap_string = |s: String| Value::String(s);
        let val_wrap_number = |n: crate::types::Number| Value::Number(n);
        let val_wrap_int = |i: i64| Value::Int(crate::types::Int(i));
        let val_wrap_object = |o: crate::types::RawObject| Value::Object(o);
        let val_wrap_byte = |b: &[u8]| Value::Byte(b.to_vec());

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => iter_typed!(
                tx,
                &self.name,
                String,
                key_wrap_string,
                String,
                val_wrap_string
            ),
            (KeyType::String, ValueType::Number) => iter_typed!(
                tx,
                &self.name,
                String,
                key_wrap_string,
                crate::types::Number,
                val_wrap_number
            ),
            (KeyType::String, ValueType::Int) => {
                iter_typed!(tx, &self.name, String, key_wrap_string, i64, val_wrap_int)
            }
            (KeyType::String, ValueType::Object) => iter_typed!(
                tx,
                &self.name,
                String,
                key_wrap_string,
                crate::types::RawObject,
                val_wrap_object
            ),
            (KeyType::String, ValueType::Byte) => iter_typed!(
                tx,
                &self.name,
                String,
                key_wrap_string,
                &[u8],
                val_wrap_byte
            ),
            (KeyType::Number, ValueType::String) => iter_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                String,
                val_wrap_string
            ),
            (KeyType::Number, ValueType::Number) => iter_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                crate::types::Number,
                val_wrap_number
            ),
            (KeyType::Number, ValueType::Int) => iter_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                i64,
                val_wrap_int
            ),
            (KeyType::Number, ValueType::Object) => iter_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                crate::types::RawObject,
                val_wrap_object
            ),
            (KeyType::Number, ValueType::Byte) => iter_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                &[u8],
                val_wrap_byte
            ),
            (KeyType::Int, ValueType::String) => {
                iter_typed!(tx, &self.name, i64, key_wrap_int, String, val_wrap_string)
            }
            (KeyType::Int, ValueType::Number) => iter_typed!(
                tx,
                &self.name,
                i64,
                key_wrap_int,
                crate::types::Number,
                val_wrap_number
            ),
            (KeyType::Int, ValueType::Int) => {
                iter_typed!(tx, &self.name, i64, key_wrap_int, i64, val_wrap_int)
            }
            (KeyType::Int, ValueType::Object) => iter_typed!(
                tx,
                &self.name,
                i64,
                key_wrap_int,
                crate::types::RawObject,
                val_wrap_object
            ),
            (KeyType::Int, ValueType::Byte) => {
                iter_typed!(tx, &self.name, i64, key_wrap_int, &[u8], val_wrap_byte)
            }
        }
    }

    fn keys(&self, tx: &redb::ReadTransaction) -> Result<Vec<Key>, TransactionError> {
        let key_wrap_string = |s: String| Key::String(s);
        let key_wrap_number = |n: crate::types::Number| Key::Number(n);
        let key_wrap_int = |i: i64| Key::Int(crate::types::Int(i));

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => {
                keys_typed!(tx, &self.name, String, key_wrap_string, String)
            }
            (KeyType::String, ValueType::Number) => keys_typed!(
                tx,
                &self.name,
                String,
                key_wrap_string,
                crate::types::Number
            ),
            (KeyType::String, ValueType::Int) => {
                keys_typed!(tx, &self.name, String, key_wrap_string, i64)
            }
            (KeyType::String, ValueType::Object) => keys_typed!(
                tx,
                &self.name,
                String,
                key_wrap_string,
                crate::types::RawObject
            ),
            (KeyType::String, ValueType::Byte) => {
                keys_typed!(tx, &self.name, String, key_wrap_string, &[u8])
            }
            (KeyType::Number, ValueType::String) => keys_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                String
            ),
            (KeyType::Number, ValueType::Number) => keys_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                crate::types::Number
            ),
            (KeyType::Number, ValueType::Int) => {
                keys_typed!(tx, &self.name, crate::types::Number, key_wrap_number, i64)
            }
            (KeyType::Number, ValueType::Object) => keys_typed!(
                tx,
                &self.name,
                crate::types::Number,
                key_wrap_number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => {
                keys_typed!(tx, &self.name, crate::types::Number, key_wrap_number, &[u8])
            }
            (KeyType::Int, ValueType::String) => {
                keys_typed!(tx, &self.name, i64, key_wrap_int, String)
            }
            (KeyType::Int, ValueType::Number) => {
                keys_typed!(tx, &self.name, i64, key_wrap_int, crate::types::Number)
            }
            (KeyType::Int, ValueType::Int) => {
                keys_typed!(tx, &self.name, i64, key_wrap_int, i64)
            }
            (KeyType::Int, ValueType::Object) => {
                keys_typed!(tx, &self.name, i64, key_wrap_int, crate::types::RawObject)
            }
            (KeyType::Int, ValueType::Byte) => {
                keys_typed!(tx, &self.name, i64, key_wrap_int, &[u8])
            }
        }
    }

    fn values(&self, tx: &redb::ReadTransaction) -> Result<Vec<Value>, TransactionError> {
        let val_wrap_string = |s: String| Value::String(s);
        let val_wrap_number = |n: crate::types::Number| Value::Number(n);
        let val_wrap_int = |i: i64| Value::Int(crate::types::Int(i));
        let val_wrap_object = |o: crate::types::RawObject| Value::Object(o);
        let val_wrap_byte = |b: &[u8]| Value::Byte(b.to_vec());

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => {
                values_typed!(tx, &self.name, String, String, val_wrap_string)
            }
            (KeyType::String, ValueType::Number) => values_typed!(
                tx,
                &self.name,
                String,
                crate::types::Number,
                val_wrap_number
            ),
            (KeyType::String, ValueType::Int) => {
                values_typed!(tx, &self.name, String, i64, val_wrap_int)
            }
            (KeyType::String, ValueType::Object) => values_typed!(
                tx,
                &self.name,
                String,
                crate::types::RawObject,
                val_wrap_object
            ),
            (KeyType::String, ValueType::Byte) => {
                values_typed!(tx, &self.name, String, &[u8], val_wrap_byte)
            }
            (KeyType::Number, ValueType::String) => values_typed!(
                tx,
                &self.name,
                crate::types::Number,
                String,
                val_wrap_string
            ),
            (KeyType::Number, ValueType::Number) => values_typed!(
                tx,
                &self.name,
                crate::types::Number,
                crate::types::Number,
                val_wrap_number
            ),
            (KeyType::Number, ValueType::Int) => {
                values_typed!(tx, &self.name, crate::types::Number, i64, val_wrap_int)
            }
            (KeyType::Number, ValueType::Object) => values_typed!(
                tx,
                &self.name,
                crate::types::Number,
                crate::types::RawObject,
                val_wrap_object
            ),
            (KeyType::Number, ValueType::Byte) => {
                values_typed!(tx, &self.name, crate::types::Number, &[u8], val_wrap_byte)
            }
            (KeyType::Int, ValueType::String) => {
                values_typed!(tx, &self.name, i64, String, val_wrap_string)
            }
            (KeyType::Int, ValueType::Number) => {
                values_typed!(tx, &self.name, i64, crate::types::Number, val_wrap_number)
            }
            (KeyType::Int, ValueType::Int) => {
                values_typed!(tx, &self.name, i64, i64, val_wrap_int)
            }
            (KeyType::Int, ValueType::Object) => values_typed!(
                tx,
                &self.name,
                i64,
                crate::types::RawObject,
                val_wrap_object
            ),
            (KeyType::Int, ValueType::Byte) => {
                values_typed!(tx, &self.name, i64, &[u8], val_wrap_byte)
            }
        }
    }

    fn get_range(
        &self,
        tx: &redb::ReadTransaction,
        start: &Key,
        end: &Key,
    ) -> Result<Vec<(Key, Value)>, TransactionError> {
        if self.key_type != KeyType::String && self.key_type != KeyType::Int {
            return Err(TransactionError::RangeNotSupported);
        }

        if start.as_type() != self.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.key_type,
                actual: start.as_type(),
            });
        }
        if end.as_type() != self.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.key_type,
                actual: end.as_type(),
            });
        }

        let key_wrap_string = |s: String| Key::String(s);
        let key_wrap_int = |i: i64| Key::Int(crate::types::Int(i));
        let val_wrap_string = |s: String| Value::String(s);
        let val_wrap_number = |n: crate::types::Number| Value::Number(n);
        let val_wrap_int = |i: i64| Value::Int(crate::types::Int(i));
        let val_wrap_object = |o: crate::types::RawObject| Value::Object(o);
        let val_wrap_byte = |b: &[u8]| Value::Byte(b.to_vec());

        match self.key_type {
            KeyType::String => {
                let start_string: String = start
                    .clone()
                    .try_into()
                    .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a String key"));
                let end_string: String = end
                    .clone()
                    .try_into()
                    .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a String key"));
                match self.value_type {
                    ValueType::String => range_typed!(
                        tx,
                        &self.name,
                        start_string,
                        end_string,
                        String,
                        key_wrap_string,
                        String,
                        val_wrap_string
                    ),
                    ValueType::Number => range_typed!(
                        tx,
                        &self.name,
                        start_string,
                        end_string,
                        String,
                        key_wrap_string,
                        crate::types::Number,
                        val_wrap_number
                    ),
                    ValueType::Int => range_typed!(
                        tx,
                        &self.name,
                        start_string,
                        end_string,
                        String,
                        key_wrap_string,
                        i64,
                        val_wrap_int
                    ),
                    ValueType::Object => range_typed!(
                        tx,
                        &self.name,
                        start_string,
                        end_string,
                        String,
                        key_wrap_string,
                        crate::types::RawObject,
                        val_wrap_object
                    ),
                    ValueType::Byte => range_typed!(
                        tx,
                        &self.name,
                        start_string,
                        end_string,
                        String,
                        key_wrap_string,
                        &[u8],
                        val_wrap_byte
                    ),
                }
            }
            KeyType::Int => {
                let start_int: i64 = <crate::types::Int as TryFrom<Key>>::try_from(start.clone())
                    .unwrap_or_else(|_| unreachable!("Validated key_type guarantees an Int key"))
                    .0;
                let end_int: i64 = <crate::types::Int as TryFrom<Key>>::try_from(end.clone())
                    .unwrap_or_else(|_| unreachable!("Validated key_type guarantees an Int key"))
                    .0;
                match self.value_type {
                    ValueType::String => range_typed!(
                        tx,
                        &self.name,
                        start_int,
                        end_int,
                        i64,
                        key_wrap_int,
                        String,
                        val_wrap_string
                    ),
                    ValueType::Number => range_typed!(
                        tx,
                        &self.name,
                        start_int,
                        end_int,
                        i64,
                        key_wrap_int,
                        crate::types::Number,
                        val_wrap_number
                    ),
                    ValueType::Int => range_typed!(
                        tx,
                        &self.name,
                        start_int,
                        end_int,
                        i64,
                        key_wrap_int,
                        i64,
                        val_wrap_int
                    ),
                    ValueType::Object => range_typed!(
                        tx,
                        &self.name,
                        start_int,
                        end_int,
                        i64,
                        key_wrap_int,
                        crate::types::RawObject,
                        val_wrap_object
                    ),
                    ValueType::Byte => range_typed!(
                        tx,
                        &self.name,
                        start_int,
                        end_int,
                        i64,
                        key_wrap_int,
                        &[u8],
                        val_wrap_byte
                    ),
                }
            }
            KeyType::Number => Err(TransactionError::RangeNotSupported),
        }
    }
}
