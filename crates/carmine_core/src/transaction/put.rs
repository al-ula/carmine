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

pub trait PutTxn<'a> {
    fn put(&self, key: Key, value: Value) -> Result<()>;
}

impl<'a> PutTxn<'a> for Transaction<'a> {
    fn put(&self, key: Key, value: Value) -> Result<()> {
        let database = self.tub.open().map_err(|e| Error::Tub(e.into()))?;
        let write_txn = database
            .begin_write()
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

        let val_to_string = |v: Value| -> String {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Bucket value_type guarantees a String value"))
        };
        let val_to_number = |v: Value| -> Number {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Bucket value_type guarantees a Number value"))
        };
        let val_to_int = |v: Value| -> i64 {
            let i: Int = v
                .try_into()
                .unwrap_or_else(|_| unreachable!("Bucket value_type guarantees an Int value"));
            *i
        };
        let val_to_object = |v: Value| -> RawObject {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Bucket value_type guarantees an Object value"))
        };
        let val_to_byte = |v: Value| -> Vec<u8> {
            v.try_into()
                .unwrap_or_else(|_| unreachable!("Bucket value_type guarantees a Byte value"))
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
}
