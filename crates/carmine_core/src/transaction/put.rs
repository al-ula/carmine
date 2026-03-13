use crate::error::Error;
use redb::{ReadableTable, TableDefinition};

use super::{Transaction, TransactionError};

use crate::{
    key::{Key, KeyType},
    types::{Int, Number, RawObject},
    value::{Value, ValueType},
};

type Result<T> = std::result::Result<T, Error>;

macro_rules! put_typed {
    ($write_txn:expr, $bucket_name:expr, $key_expr:expr, $val_expr:expr, $KeyRedb:ty, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($bucket_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(|e| Error::Bucket(e.into()))?;
        let key = $key_expr;
        if table_handle
            .get(&key)
            .map_err(|e| Error::Transaction(TransactionError::StorageError(e)))?
            .is_some()
        {
            return Err(Error::Transaction(TransactionError::KeyAlreadyExists));
        }
        table_handle
            .insert(key, $val_expr)
            .map_err(|e| Error::Transaction(e.into()))?;
    }};
}

macro_rules! batch_put_typed {
    ($write_txn:expr, $bucket_name:expr, $entries:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_conv:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($bucket_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(|e| Error::Bucket(e.into()))?;

        let mut results = Vec::with_capacity($entries.len());

        for (key, value) in $entries.iter().cloned() {
            let key = match $key_conv(key) {
                Ok(key) => key,
                Err(error) => {
                    results.push(Err(Error::Key(error)));
                    continue;
                }
            };

            let value = match $val_conv(value) {
                Ok(value) => value,
                Err(error) => {
                    results.push(Err(Error::Value(error)));
                    continue;
                }
            };

            if table_handle
                .get(&key)
                .map_err(|e| Error::Transaction(TransactionError::StorageError(e)))?
                .is_some()
            {
                results.push(Err(Error::Transaction(TransactionError::KeyAlreadyExists)));
                continue;
            }

            table_handle
                .insert(key, value)
                .map_err(|e| Error::Transaction(e.into()))?;
            results.push(Ok(()));
        }

        Ok(results)
    }};
}

macro_rules! batch_put_typed_bytes {
    ($write_txn:expr, $bucket_name:expr, $entries:expr,
     $KeyRedb:ty, $key_conv:expr,
     $val_conv:expr) => {{
        let table: TableDefinition<$KeyRedb, &[u8]> = TableDefinition::new($bucket_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(|e| Error::Bucket(e.into()))?;

        let mut results = Vec::with_capacity($entries.len());

        for (key, value) in $entries.iter().cloned() {
            let key = match $key_conv(key) {
                Ok(key) => key,
                Err(error) => {
                    results.push(Err(Error::Key(error)));
                    continue;
                }
            };

            let value = match $val_conv(value) {
                Ok(value) => value,
                Err(error) => {
                    results.push(Err(Error::Value(error)));
                    continue;
                }
            };

            if table_handle
                .get(&key)
                .map_err(|e| Error::Transaction(TransactionError::StorageError(e)))?
                .is_some()
            {
                results.push(Err(Error::Transaction(TransactionError::KeyAlreadyExists)));
                continue;
            }

            table_handle
                .insert(key, value.as_slice())
                .map_err(|e| Error::Transaction(e.into()))?;
            results.push(Ok(()));
        }

        Ok(results)
    }};
}

pub trait PutTxn<'a> {
    fn put(&self, key: Key, value: Value) -> Result<()>;
    fn batch_put(&self, entries: &[(Key, Value)]) -> Result<Vec<Result<()>>>;
}

impl<'a> PutTxn<'a> for Transaction<'a> {
    fn put(&self, key: Key, value: Value) -> Result<()> {
        if let Err(e) = self.validate_key_type(&key) {
            return Err(Error::Transaction(e));
        }
        if let Err(e) = self.validate_value_type(&value) {
            return Err(Error::Transaction(e));
        }

        let database = self.tub.open().map_err(|e| Error::Tub(e.into()))?;
        let write_txn = database
            .begin_write()
            .map_err(|e| Error::Transaction(e.into()))?;

        let name = &self.bucket.name;

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

        let val_to_string = |v: Value| -> String {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Validated value_type guarantees a String value"))
        };
        let val_to_number = |v: Value| -> Number {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Validated value_type guarantees a Number value"))
        };
        let val_to_int = |v: Value| -> i64 {
            let i: Int = v
                .try_into()
                .unwrap_or_else(|_| unreachable!("Validated value_type guarantees an Int value"));
            *i
        };
        let val_to_object = |v: Value| -> RawObject {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Validated value_type guarantees an Object value"))
        };
        let val_to_byte = |v: Value| -> Vec<u8> {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Validated value_type guarantees a Byte value"))
        };

        match (self.bucket.key_type, self.bucket.value_type) {
            (KeyType::String, ValueType::String) => put_typed!(
                write_txn,
                name,
                key_to_string(key),
                val_to_string(value),
                String,
                String
            ),
            (KeyType::String, ValueType::Number) => put_typed!(
                write_txn,
                name,
                key_to_string(key),
                val_to_number(value),
                String,
                Number
            ),
            (KeyType::String, ValueType::Int) => put_typed!(
                write_txn,
                name,
                key_to_string(key),
                val_to_int(value),
                String,
                Int
            ),
            (KeyType::String, ValueType::Object) => put_typed!(
                write_txn,
                name,
                key_to_string(key),
                val_to_object(value),
                String,
                RawObject
            ),
            (KeyType::String, ValueType::Byte) => {
                let k = key_to_string(key);
                let v = val_to_byte(value);
                put_typed!(write_txn, name, k, v.as_slice(), String, &[u8]);
            }

            (KeyType::Number, ValueType::String) => put_typed!(
                write_txn,
                name,
                key_to_number(key),
                val_to_string(value),
                Number,
                String
            ),
            (KeyType::Number, ValueType::Number) => put_typed!(
                write_txn,
                name,
                key_to_number(key),
                val_to_number(value),
                Number,
                Number
            ),
            (KeyType::Number, ValueType::Int) => put_typed!(
                write_txn,
                name,
                key_to_number(key),
                val_to_int(value),
                Number,
                Int
            ),
            (KeyType::Number, ValueType::Object) => put_typed!(
                write_txn,
                name,
                key_to_number(key),
                val_to_object(value),
                Number,
                RawObject
            ),
            (KeyType::Number, ValueType::Byte) => {
                let k = key_to_number(key);
                let v = val_to_byte(value);
                put_typed!(write_txn, name, k, v.as_slice(), Number, &[u8]);
            }

            (KeyType::Int, ValueType::String) => put_typed!(
                write_txn,
                name,
                key_to_int(key),
                val_to_string(value),
                Int,
                String
            ),
            (KeyType::Int, ValueType::Number) => put_typed!(
                write_txn,
                name,
                key_to_int(key),
                val_to_number(value),
                Int,
                Number
            ),
            (KeyType::Int, ValueType::Int) => put_typed!(
                write_txn,
                name,
                key_to_int(key),
                val_to_int(value),
                Int,
                Int
            ),
            (KeyType::Int, ValueType::Object) => put_typed!(
                write_txn,
                name,
                key_to_int(key),
                val_to_object(value),
                Int,
                RawObject
            ),
            (KeyType::Int, ValueType::Byte) => {
                let k = key_to_int(key);
                let v = val_to_byte(value);
                put_typed!(write_txn, name, k, v.as_slice(), Int, &[u8]);
            }
        }

        write_txn
            .commit()
            .map_err(|e| Error::Transaction(e.into()))?;
        Ok(())
    }

    fn batch_put(&self, entries: &[(Key, Value)]) -> Result<Vec<Result<()>>> {
        let bucket_key_type = self.bucket.key_type;
        let bucket_value_type = self.bucket.value_type;

        let mut validation_errors: Vec<Option<Error>> = Vec::with_capacity(entries.len());
        for (key, value) in entries.iter() {
            let key_type = key.as_type();
            let value_type = value.as_type();

            let key_err = if key_type != bucket_key_type {
                Some(Error::Transaction(TransactionError::KeyTypeMismatch {
                    expected: bucket_key_type,
                    actual: key_type,
                }))
            } else {
                None
            };

            let value_err = if value_type != bucket_value_type {
                Some(Error::Transaction(TransactionError::ValueTypeMismatch {
                    expected: bucket_value_type,
                    actual: value_type,
                }))
            } else {
                None
            };

            validation_errors.push(key_err.or(value_err));
        }

        let has_validation_errors = validation_errors.iter().any(|e| e.is_some());
        if has_validation_errors {
            let results: Vec<Result<()>> = validation_errors
                .into_iter()
                .map(|err| match err {
                    Some(e) => Err(e),
                    None => Ok(()),
                })
                .collect();
            return Ok(results);
        }

        let database = self.tub.open().map_err(|e| Error::Tub(e.into()))?;
        let write_txn = database
            .begin_write()
            .map_err(|e| Error::Transaction(e.into()))?;

        let name = &self.bucket.name;

        let safe_key_to_string =
            |k: Key| -> std::result::Result<String, crate::key::KeyError> { k.try_into() };
        let safe_key_to_number =
            |k: Key| -> std::result::Result<Number, crate::key::KeyError> { k.try_into() };
        let safe_key_to_int = |k: Key| -> std::result::Result<i64, crate::key::KeyError> {
            let i: std::result::Result<Int, crate::key::KeyError> = k.try_into();
            i.map(|v| *v)
        };

        let safe_val_to_string =
            |v: Value| -> std::result::Result<String, crate::value::ValueError> { v.try_into() };
        let safe_val_to_number =
            |v: Value| -> std::result::Result<Number, crate::value::ValueError> { v.try_into() };
        let safe_val_to_int = |v: Value| -> std::result::Result<i64, crate::value::ValueError> {
            let i: std::result::Result<Int, crate::value::ValueError> = v.try_into();
            i.map(|v| *v)
        };
        let safe_val_to_object =
            |v: Value| -> std::result::Result<RawObject, crate::value::ValueError> { v.try_into() };
        let safe_val_to_byte =
            |v: Value| -> std::result::Result<Vec<u8>, crate::value::ValueError> { v.try_into() };

        let results: Vec<Result<()>> = match (self.bucket.key_type, self.bucket.value_type) {
            (KeyType::String, ValueType::String) => batch_put_typed!(
                write_txn,
                name,
                entries,
                String,
                safe_key_to_string,
                String,
                safe_val_to_string
            ),
            (KeyType::String, ValueType::Number) => batch_put_typed!(
                write_txn,
                name,
                entries,
                String,
                safe_key_to_string,
                Number,
                safe_val_to_number
            ),
            (KeyType::String, ValueType::Int) => batch_put_typed!(
                write_txn,
                name,
                entries,
                String,
                safe_key_to_string,
                Int,
                safe_val_to_int
            ),
            (KeyType::String, ValueType::Object) => batch_put_typed!(
                write_txn,
                name,
                entries,
                String,
                safe_key_to_string,
                RawObject,
                safe_val_to_object
            ),
            (KeyType::String, ValueType::Byte) => batch_put_typed_bytes!(
                write_txn,
                name,
                entries,
                String,
                safe_key_to_string,
                safe_val_to_byte
            ),

            (KeyType::Number, ValueType::String) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Number,
                safe_key_to_number,
                String,
                safe_val_to_string
            ),
            (KeyType::Number, ValueType::Number) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Number,
                safe_key_to_number,
                Number,
                safe_val_to_number
            ),
            (KeyType::Number, ValueType::Int) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Number,
                safe_key_to_number,
                Int,
                safe_val_to_int
            ),
            (KeyType::Number, ValueType::Object) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Number,
                safe_key_to_number,
                RawObject,
                safe_val_to_object
            ),
            (KeyType::Number, ValueType::Byte) => batch_put_typed_bytes!(
                write_txn,
                name,
                entries,
                Number,
                safe_key_to_number,
                safe_val_to_byte
            ),

            (KeyType::Int, ValueType::String) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Int,
                safe_key_to_int,
                String,
                safe_val_to_string
            ),
            (KeyType::Int, ValueType::Number) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Int,
                safe_key_to_int,
                Number,
                safe_val_to_number
            ),
            (KeyType::Int, ValueType::Int) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Int,
                safe_key_to_int,
                Int,
                safe_val_to_int
            ),
            (KeyType::Int, ValueType::Object) => batch_put_typed!(
                write_txn,
                name,
                entries,
                Int,
                safe_key_to_int,
                RawObject,
                safe_val_to_object
            ),
            (KeyType::Int, ValueType::Byte) => batch_put_typed_bytes!(
                write_txn,
                name,
                entries,
                Int,
                safe_key_to_int,
                safe_val_to_byte
            ),
        }
        .map_err(|error: Error| error)?;

        write_txn
            .commit()
            .map_err(|e| Error::Transaction(e.into()))?;

        Ok(results)
    }
}
