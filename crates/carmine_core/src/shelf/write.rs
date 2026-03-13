use super::Shelf;
use crate::key::{Key, KeyType};
use crate::transaction::{TransactionError, Writable};
use crate::value::{Value, ValueType};
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};

// --- Single-operation macros ---

macro_rules! set_typed {
    ($write_txn:expr, $shelf_name:expr, $key_expr:expr, $val_expr:expr, $KeyRedb:ty, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        table_handle
            .insert($key_expr, $val_expr)
            .map_err(TransactionError::from)?;
        Ok(())
    }};
}

macro_rules! put_typed {
    ($write_txn:expr, $shelf_name:expr, $key_expr:expr, $val_expr:expr, $KeyRedb:ty, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let key = $key_expr;
        if table_handle
            .get(&key)
            .map_err(TransactionError::from)?
            .is_some()
        {
            return Err(TransactionError::KeyAlreadyExists);
        }
        table_handle
            .insert(key, $val_expr)
            .map_err(TransactionError::from)?;
        Ok(())
    }};
}

macro_rules! delete_typed {
    ($write_txn:expr, $shelf_name:expr, $key_expr:expr, $KeyRedb:ty, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let removed = table_handle
            .remove($key_expr)
            .map_err(TransactionError::from)?;
        Ok(removed.is_some())
    }};
}

macro_rules! clear_typed {
    ($write_txn:expr, $shelf_name:expr, $KeyRedb:ty, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let count = table_handle.len().map_err(TransactionError::from)?;
        table_handle
            .retain(|_, _| false)
            .map_err(TransactionError::from)?;
        Ok(count)
    }};
}

// --- Batch-operation macros ---

macro_rules! batch_set_typed {
    ($write_txn:expr, $shelf_name:expr, $entries:expr, $key_type:expr, $val_type:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_conv:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut results = Vec::with_capacity($entries.len());
        for (key, value) in $entries.iter() {
            if key.as_type() != $key_type {
                results.push(Err(TransactionError::KeyTypeMismatch {
                    expected: $key_type,
                    actual: key.as_type(),
                }));
                continue;
            }
            if value.as_type() != $val_type {
                results.push(Err(TransactionError::ValueTypeMismatch {
                    expected: $val_type,
                    actual: value.as_type(),
                }));
                continue;
            }
            let k = $key_conv(key.clone());
            let v = $val_conv(value.clone());
            match table_handle.insert(k, v) {
                Ok(_) => results.push(Ok(())),
                Err(e) => results.push(Err(TransactionError::from(e))),
            }
        }
        Ok(results)
    }};
}

macro_rules! batch_set_typed_bytes {
    ($write_txn:expr, $shelf_name:expr, $entries:expr, $key_type:expr, $val_type:expr,
     $KeyRedb:ty, $key_conv:expr,
     $val_conv:expr) => {{
        let table: TableDefinition<$KeyRedb, &[u8]> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut results = Vec::with_capacity($entries.len());
        for (key, value) in $entries.iter() {
            if key.as_type() != $key_type {
                results.push(Err(TransactionError::KeyTypeMismatch {
                    expected: $key_type,
                    actual: key.as_type(),
                }));
                continue;
            }
            if value.as_type() != $val_type {
                results.push(Err(TransactionError::ValueTypeMismatch {
                    expected: $val_type,
                    actual: value.as_type(),
                }));
                continue;
            }
            let k = $key_conv(key.clone());
            let v = $val_conv(value.clone());
            match table_handle.insert(k, v.as_slice()) {
                Ok(_) => results.push(Ok(())),
                Err(e) => results.push(Err(TransactionError::from(e))),
            }
        }
        Ok(results)
    }};
}

macro_rules! batch_put_typed {
    ($write_txn:expr, $shelf_name:expr, $entries:expr, $key_type:expr, $val_type:expr,
     $KeyRedb:ty, $key_conv:expr,
     $ValRedb:ty, $val_conv:expr) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut results = Vec::with_capacity($entries.len());
        for (key, value) in $entries.iter() {
            if key.as_type() != $key_type {
                results.push(Err(TransactionError::KeyTypeMismatch {
                    expected: $key_type,
                    actual: key.as_type(),
                }));
                continue;
            }
            if value.as_type() != $val_type {
                results.push(Err(TransactionError::ValueTypeMismatch {
                    expected: $val_type,
                    actual: value.as_type(),
                }));
                continue;
            }
            let k = $key_conv(key.clone());
            if table_handle
                .get(&k)
                .map_err(TransactionError::from)?
                .is_some()
            {
                results.push(Err(TransactionError::KeyAlreadyExists));
                continue;
            }
            let v = $val_conv(value.clone());
            match table_handle.insert(k, v) {
                Ok(_) => results.push(Ok(())),
                Err(e) => results.push(Err(TransactionError::from(e))),
            }
        }
        Ok(results)
    }};
}

macro_rules! batch_put_typed_bytes {
    ($write_txn:expr, $shelf_name:expr, $entries:expr, $key_type:expr, $val_type:expr,
     $KeyRedb:ty, $key_conv:expr,
     $val_conv:expr) => {{
        let table: TableDefinition<$KeyRedb, &[u8]> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut results = Vec::with_capacity($entries.len());
        for (key, value) in $entries.iter() {
            if key.as_type() != $key_type {
                results.push(Err(TransactionError::KeyTypeMismatch {
                    expected: $key_type,
                    actual: key.as_type(),
                }));
                continue;
            }
            if value.as_type() != $val_type {
                results.push(Err(TransactionError::ValueTypeMismatch {
                    expected: $val_type,
                    actual: value.as_type(),
                }));
                continue;
            }
            let k = $key_conv(key.clone());
            if table_handle
                .get(&k)
                .map_err(TransactionError::from)?
                .is_some()
            {
                results.push(Err(TransactionError::KeyAlreadyExists));
                continue;
            }
            let v = $val_conv(value.clone());
            match table_handle.insert(k, v.as_slice()) {
                Ok(_) => results.push(Ok(())),
                Err(e) => results.push(Err(TransactionError::from(e))),
            }
        }
        Ok(results)
    }};
}

macro_rules! batch_delete_typed {
    ($write_txn:expr, $shelf_name:expr, $keys:expr, $key_type:expr,
     $KeyRedb:ty, $key_conv:expr, $ValRedb:ty) => {{
        let table: TableDefinition<$KeyRedb, $ValRedb> = TableDefinition::new($shelf_name);
        let mut table_handle = $write_txn
            .open_table(table)
            .map_err(TransactionError::from)?;
        let mut results = Vec::with_capacity($keys.len());
        for key in $keys.iter() {
            if key.as_type() != $key_type {
                results.push(false);
                continue;
            }
            let k = $key_conv(key.clone());
            let removed = table_handle.remove(k).map_err(TransactionError::from)?;
            results.push(removed.is_some());
        }
        Ok(results)
    }};
}

// --- Key/Value converters ---

fn key_to_string(k: Key) -> String {
    k.try_into()
        .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a String key"))
}

fn key_to_number(k: Key) -> crate::types::Number {
    k.try_into()
        .unwrap_or_else(|_| unreachable!("Validated key_type guarantees a Number key"))
}

fn key_to_int(k: Key) -> i64 {
    let i: crate::types::Int = k
        .try_into()
        .unwrap_or_else(|_| unreachable!("Validated key_type guarantees an Int key"));
    *i
}

fn val_to_string(v: Value) -> String {
    v.try_into()
        .unwrap_or_else(|_| unreachable!("Validated value_type guarantees a String value"))
}

fn val_to_number(v: Value) -> crate::types::Number {
    v.try_into()
        .unwrap_or_else(|_| unreachable!("Validated value_type guarantees a Number value"))
}

fn val_to_int(v: Value) -> i64 {
    let i: crate::types::Int = v
        .try_into()
        .unwrap_or_else(|_| unreachable!("Validated value_type guarantees an Int value"));
    *i
}

fn val_to_object(v: Value) -> crate::types::RawObject {
    v.try_into()
        .unwrap_or_else(|_| unreachable!("Validated value_type guarantees an Object value"))
}

fn val_to_byte(v: Value) -> Vec<u8> {
    v.try_into()
        .unwrap_or_else(|_| unreachable!("Validated value_type guarantees a Byte value"))
}

// --- Writable implementation ---

impl Writable for Shelf {
    fn set(
        &self,
        tx: &redb::WriteTransaction,
        key: Key,
        value: Value,
    ) -> Result<(), TransactionError> {
        if key.as_type() != self.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.key_type,
                actual: key.as_type(),
            });
        }
        if value.as_type() != self.value_type {
            return Err(TransactionError::ValueTypeMismatch {
                expected: self.value_type,
                actual: value.as_type(),
            });
        }

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => set_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_string(value),
                String,
                String
            ),
            (KeyType::String, ValueType::Number) => set_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_number(value),
                String,
                crate::types::Number
            ),
            (KeyType::String, ValueType::Int) => set_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_int(value),
                String,
                i64
            ),
            (KeyType::String, ValueType::Object) => set_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_object(value),
                String,
                crate::types::RawObject
            ),
            (KeyType::String, ValueType::Byte) => {
                let k = key_to_string(key);
                let v = val_to_byte(value);
                set_typed!(tx, &self.name, k, v.as_slice(), String, &[u8])
            }
            (KeyType::Number, ValueType::String) => set_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_string(value),
                crate::types::Number,
                String
            ),
            (KeyType::Number, ValueType::Number) => set_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_number(value),
                crate::types::Number,
                crate::types::Number
            ),
            (KeyType::Number, ValueType::Int) => set_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_int(value),
                crate::types::Number,
                i64
            ),
            (KeyType::Number, ValueType::Object) => set_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_object(value),
                crate::types::Number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => {
                let k = key_to_number(key);
                let v = val_to_byte(value);
                set_typed!(tx, &self.name, k, v.as_slice(), crate::types::Number, &[u8])
            }
            (KeyType::Int, ValueType::String) => set_typed!(
                tx,
                &self.name,
                key_to_int(key),
                val_to_string(value),
                i64,
                String
            ),
            (KeyType::Int, ValueType::Number) => set_typed!(
                tx,
                &self.name,
                key_to_int(key),
                val_to_number(value),
                i64,
                crate::types::Number
            ),
            (KeyType::Int, ValueType::Int) => {
                set_typed!(tx, &self.name, key_to_int(key), val_to_int(value), i64, i64)
            }
            (KeyType::Int, ValueType::Object) => set_typed!(
                tx,
                &self.name,
                key_to_int(key),
                val_to_object(value),
                i64,
                crate::types::RawObject
            ),
            (KeyType::Int, ValueType::Byte) => {
                let k = key_to_int(key);
                let v = val_to_byte(value);
                set_typed!(tx, &self.name, k, v.as_slice(), i64, &[u8])
            }
        }
    }

    fn put(
        &self,
        tx: &redb::WriteTransaction,
        key: Key,
        value: Value,
    ) -> Result<(), TransactionError> {
        if key.as_type() != self.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.key_type,
                actual: key.as_type(),
            });
        }
        if value.as_type() != self.value_type {
            return Err(TransactionError::ValueTypeMismatch {
                expected: self.value_type,
                actual: value.as_type(),
            });
        }

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => put_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_string(value),
                String,
                String
            ),
            (KeyType::String, ValueType::Number) => put_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_number(value),
                String,
                crate::types::Number
            ),
            (KeyType::String, ValueType::Int) => put_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_int(value),
                String,
                i64
            ),
            (KeyType::String, ValueType::Object) => put_typed!(
                tx,
                &self.name,
                key_to_string(key),
                val_to_object(value),
                String,
                crate::types::RawObject
            ),
            (KeyType::String, ValueType::Byte) => {
                let k = key_to_string(key);
                let v = val_to_byte(value);
                put_typed!(tx, &self.name, k, v.as_slice(), String, &[u8])
            }
            (KeyType::Number, ValueType::String) => put_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_string(value),
                crate::types::Number,
                String
            ),
            (KeyType::Number, ValueType::Number) => put_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_number(value),
                crate::types::Number,
                crate::types::Number
            ),
            (KeyType::Number, ValueType::Int) => put_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_int(value),
                crate::types::Number,
                i64
            ),
            (KeyType::Number, ValueType::Object) => put_typed!(
                tx,
                &self.name,
                key_to_number(key),
                val_to_object(value),
                crate::types::Number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => {
                let k = key_to_number(key);
                let v = val_to_byte(value);
                put_typed!(tx, &self.name, k, v.as_slice(), crate::types::Number, &[u8])
            }
            (KeyType::Int, ValueType::String) => put_typed!(
                tx,
                &self.name,
                key_to_int(key),
                val_to_string(value),
                i64,
                String
            ),
            (KeyType::Int, ValueType::Number) => put_typed!(
                tx,
                &self.name,
                key_to_int(key),
                val_to_number(value),
                i64,
                crate::types::Number
            ),
            (KeyType::Int, ValueType::Int) => {
                put_typed!(tx, &self.name, key_to_int(key), val_to_int(value), i64, i64)
            }
            (KeyType::Int, ValueType::Object) => put_typed!(
                tx,
                &self.name,
                key_to_int(key),
                val_to_object(value),
                i64,
                crate::types::RawObject
            ),
            (KeyType::Int, ValueType::Byte) => {
                let k = key_to_int(key);
                let v = val_to_byte(value);
                put_typed!(tx, &self.name, k, v.as_slice(), i64, &[u8])
            }
        }
    }

    fn delete(&self, tx: &redb::WriteTransaction, key: &Key) -> Result<bool, TransactionError> {
        if key.as_type() != self.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.key_type,
                actual: key.as_type(),
            });
        }

        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => {
                delete_typed!(tx, &self.name, key_to_string(key.clone()), String, String)
            }
            (KeyType::String, ValueType::Number) => delete_typed!(
                tx,
                &self.name,
                key_to_string(key.clone()),
                String,
                crate::types::Number
            ),
            (KeyType::String, ValueType::Int) => {
                delete_typed!(tx, &self.name, key_to_string(key.clone()), String, i64)
            }
            (KeyType::String, ValueType::Object) => delete_typed!(
                tx,
                &self.name,
                key_to_string(key.clone()),
                String,
                crate::types::RawObject
            ),
            (KeyType::String, ValueType::Byte) => {
                delete_typed!(tx, &self.name, key_to_string(key.clone()), String, &[u8])
            }
            (KeyType::Number, ValueType::String) => delete_typed!(
                tx,
                &self.name,
                key_to_number(key.clone()),
                crate::types::Number,
                String
            ),
            (KeyType::Number, ValueType::Number) => delete_typed!(
                tx,
                &self.name,
                key_to_number(key.clone()),
                crate::types::Number,
                crate::types::Number
            ),
            (KeyType::Number, ValueType::Int) => delete_typed!(
                tx,
                &self.name,
                key_to_number(key.clone()),
                crate::types::Number,
                i64
            ),
            (KeyType::Number, ValueType::Object) => delete_typed!(
                tx,
                &self.name,
                key_to_number(key.clone()),
                crate::types::Number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => delete_typed!(
                tx,
                &self.name,
                key_to_number(key.clone()),
                crate::types::Number,
                &[u8]
            ),
            (KeyType::Int, ValueType::String) => {
                delete_typed!(tx, &self.name, key_to_int(key.clone()), i64, String)
            }
            (KeyType::Int, ValueType::Number) => delete_typed!(
                tx,
                &self.name,
                key_to_int(key.clone()),
                i64,
                crate::types::Number
            ),
            (KeyType::Int, ValueType::Int) => {
                delete_typed!(tx, &self.name, key_to_int(key.clone()), i64, i64)
            }
            (KeyType::Int, ValueType::Object) => delete_typed!(
                tx,
                &self.name,
                key_to_int(key.clone()),
                i64,
                crate::types::RawObject
            ),
            (KeyType::Int, ValueType::Byte) => {
                delete_typed!(tx, &self.name, key_to_int(key.clone()), i64, &[u8])
            }
        }
    }

    fn batch_set(
        &self,
        tx: &redb::WriteTransaction,
        entries: &[(Key, Value)],
    ) -> Result<Vec<Result<(), TransactionError>>, TransactionError> {
        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                String,
                val_to_string
            ),
            (KeyType::String, ValueType::Number) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                crate::types::Number,
                val_to_number
            ),
            (KeyType::String, ValueType::Int) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                i64,
                val_to_int
            ),
            (KeyType::String, ValueType::Object) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                crate::types::RawObject,
                val_to_object
            ),
            (KeyType::String, ValueType::Byte) => batch_set_typed_bytes!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                val_to_byte
            ),
            (KeyType::Number, ValueType::String) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                String,
                val_to_string
            ),
            (KeyType::Number, ValueType::Number) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                crate::types::Number,
                val_to_number
            ),
            (KeyType::Number, ValueType::Int) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                i64,
                val_to_int
            ),
            (KeyType::Number, ValueType::Object) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                crate::types::RawObject,
                val_to_object
            ),
            (KeyType::Number, ValueType::Byte) => batch_set_typed_bytes!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                val_to_byte
            ),
            (KeyType::Int, ValueType::String) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                String,
                val_to_string
            ),
            (KeyType::Int, ValueType::Number) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                crate::types::Number,
                val_to_number
            ),
            (KeyType::Int, ValueType::Int) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                i64,
                val_to_int
            ),
            (KeyType::Int, ValueType::Object) => batch_set_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                crate::types::RawObject,
                val_to_object
            ),
            (KeyType::Int, ValueType::Byte) => batch_set_typed_bytes!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                val_to_byte
            ),
        }
    }

    fn batch_put(
        &self,
        tx: &redb::WriteTransaction,
        entries: &[(Key, Value)],
    ) -> Result<Vec<Result<(), TransactionError>>, TransactionError> {
        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                String,
                val_to_string
            ),
            (KeyType::String, ValueType::Number) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                crate::types::Number,
                val_to_number
            ),
            (KeyType::String, ValueType::Int) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                i64,
                val_to_int
            ),
            (KeyType::String, ValueType::Object) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                crate::types::RawObject,
                val_to_object
            ),
            (KeyType::String, ValueType::Byte) => batch_put_typed_bytes!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                String,
                key_to_string,
                val_to_byte
            ),
            (KeyType::Number, ValueType::String) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                String,
                val_to_string
            ),
            (KeyType::Number, ValueType::Number) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                crate::types::Number,
                val_to_number
            ),
            (KeyType::Number, ValueType::Int) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                i64,
                val_to_int
            ),
            (KeyType::Number, ValueType::Object) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                crate::types::RawObject,
                val_to_object
            ),
            (KeyType::Number, ValueType::Byte) => batch_put_typed_bytes!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                crate::types::Number,
                key_to_number,
                val_to_byte
            ),
            (KeyType::Int, ValueType::String) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                String,
                val_to_string
            ),
            (KeyType::Int, ValueType::Number) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                crate::types::Number,
                val_to_number
            ),
            (KeyType::Int, ValueType::Int) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                i64,
                val_to_int
            ),
            (KeyType::Int, ValueType::Object) => batch_put_typed!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                crate::types::RawObject,
                val_to_object
            ),
            (KeyType::Int, ValueType::Byte) => batch_put_typed_bytes!(
                tx,
                &self.name,
                entries,
                self.key_type,
                self.value_type,
                i64,
                key_to_int,
                val_to_byte
            ),
        }
    }

    fn batch_delete(
        &self,
        tx: &redb::WriteTransaction,
        keys: &[Key],
    ) -> Result<Vec<bool>, TransactionError> {
        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                String,
                key_to_string,
                String
            ),
            (KeyType::String, ValueType::Number) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                String,
                key_to_string,
                crate::types::Number
            ),
            (KeyType::String, ValueType::Int) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                String,
                key_to_string,
                i64
            ),
            (KeyType::String, ValueType::Object) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                String,
                key_to_string,
                crate::types::RawObject
            ),
            (KeyType::String, ValueType::Byte) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                String,
                key_to_string,
                &[u8]
            ),
            (KeyType::Number, ValueType::String) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                crate::types::Number,
                key_to_number,
                String
            ),
            (KeyType::Number, ValueType::Number) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                crate::types::Number,
                key_to_number,
                crate::types::Number
            ),
            (KeyType::Number, ValueType::Int) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                crate::types::Number,
                key_to_number,
                i64
            ),
            (KeyType::Number, ValueType::Object) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                crate::types::Number,
                key_to_number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                crate::types::Number,
                key_to_number,
                &[u8]
            ),
            (KeyType::Int, ValueType::String) => {
                batch_delete_typed!(tx, &self.name, keys, self.key_type, i64, key_to_int, String)
            }
            (KeyType::Int, ValueType::Number) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                i64,
                key_to_int,
                crate::types::Number
            ),
            (KeyType::Int, ValueType::Int) => {
                batch_delete_typed!(tx, &self.name, keys, self.key_type, i64, key_to_int, i64)
            }
            (KeyType::Int, ValueType::Object) => batch_delete_typed!(
                tx,
                &self.name,
                keys,
                self.key_type,
                i64,
                key_to_int,
                crate::types::RawObject
            ),
            (KeyType::Int, ValueType::Byte) => {
                batch_delete_typed!(tx, &self.name, keys, self.key_type, i64, key_to_int, &[u8])
            }
        }
    }

    fn clear(&self, tx: &redb::WriteTransaction) -> Result<u64, TransactionError> {
        match (self.key_type, self.value_type) {
            (KeyType::String, ValueType::String) => clear_typed!(tx, &self.name, String, String),
            (KeyType::String, ValueType::Number) => {
                clear_typed!(tx, &self.name, String, crate::types::Number)
            }
            (KeyType::String, ValueType::Int) => clear_typed!(tx, &self.name, String, i64),
            (KeyType::String, ValueType::Object) => {
                clear_typed!(tx, &self.name, String, crate::types::RawObject)
            }
            (KeyType::String, ValueType::Byte) => clear_typed!(tx, &self.name, String, &[u8]),
            (KeyType::Number, ValueType::String) => {
                clear_typed!(tx, &self.name, crate::types::Number, String)
            }
            (KeyType::Number, ValueType::Number) => {
                clear_typed!(tx, &self.name, crate::types::Number, crate::types::Number)
            }
            (KeyType::Number, ValueType::Int) => {
                clear_typed!(tx, &self.name, crate::types::Number, i64)
            }
            (KeyType::Number, ValueType::Object) => clear_typed!(
                tx,
                &self.name,
                crate::types::Number,
                crate::types::RawObject
            ),
            (KeyType::Number, ValueType::Byte) => {
                clear_typed!(tx, &self.name, crate::types::Number, &[u8])
            }
            (KeyType::Int, ValueType::String) => clear_typed!(tx, &self.name, i64, String),
            (KeyType::Int, ValueType::Number) => {
                clear_typed!(tx, &self.name, i64, crate::types::Number)
            }
            (KeyType::Int, ValueType::Int) => clear_typed!(tx, &self.name, i64, i64),
            (KeyType::Int, ValueType::Object) => {
                clear_typed!(tx, &self.name, i64, crate::types::RawObject)
            }
            (KeyType::Int, ValueType::Byte) => clear_typed!(tx, &self.name, i64, &[u8]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::{Readable, TransactionError};
    use redb::ReadableDatabase;

    fn temp_db() -> (tempfile::NamedTempFile, redb::Database) {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = redb::Database::create(file.path()).unwrap();
        (file, db)
    }

    fn test_shelf() -> Shelf {
        Shelf::new("test".to_string(), KeyType::String, ValueType::String)
    }

    #[test]
    fn test_set_get_delete_get_put_put_clear_count() {
        let (_file, db) = temp_db();
        let shelf = test_shelf();

        let key = Key::String("hello".to_string());
        let val = Value::String("world".to_string());

        // set → commit → get returns the value
        {
            let tx = db.begin_write().unwrap();
            shelf.set(&tx, key.clone(), val.clone()).unwrap();
            tx.commit().unwrap();
        }
        {
            let tx = db.begin_read().unwrap();
            let result = shelf.get(&tx, &key).unwrap();
            assert_eq!(result, Some(val.clone()));
        }

        // delete → commit → get returns None
        {
            let tx = db.begin_write().unwrap();
            let existed = shelf.delete(&tx, &key).unwrap();
            assert!(existed);
            tx.commit().unwrap();
        }
        {
            let tx = db.begin_read().unwrap();
            let result = shelf.get(&tx, &key).unwrap();
            assert_eq!(result, None);
        }

        // put succeeds on absent key
        {
            let tx = db.begin_write().unwrap();
            shelf.put(&tx, key.clone(), val.clone()).unwrap();
            tx.commit().unwrap();
        }

        // put again → KeyAlreadyExists
        {
            let tx = db.begin_write().unwrap();
            let err = shelf.put(&tx, key.clone(), val.clone()).unwrap_err();
            assert!(matches!(err, TransactionError::KeyAlreadyExists));
            // don't commit — tx is aborted on drop
        }

        // clear → commit → count returns 0
        {
            let tx = db.begin_write().unwrap();
            let cleared = shelf.clear(&tx).unwrap();
            assert_eq!(cleared, 1);
            tx.commit().unwrap();
        }
        {
            let tx = db.begin_read().unwrap();
            let count = shelf.count(&tx).unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn test_multiple_ops_single_transaction() {
        let (_file, db) = temp_db();
        let shelf = test_shelf();

        // Multiple sets in one transaction, then commit once
        {
            let tx = db.begin_write().unwrap();
            shelf
                .set(&tx, Key::String("a".into()), Value::String("1".into()))
                .unwrap();
            shelf
                .set(&tx, Key::String("b".into()), Value::String("2".into()))
                .unwrap();
            shelf
                .set(&tx, Key::String("c".into()), Value::String("3".into()))
                .unwrap();
            tx.commit().unwrap();
        }
        {
            let tx = db.begin_read().unwrap();
            assert_eq!(shelf.count(&tx).unwrap(), 3);
            assert_eq!(
                shelf.get(&tx, &Key::String("b".into())).unwrap(),
                Some(Value::String("2".into()))
            );
        }
    }

    #[test]
    fn test_type_validation() {
        let (_file, db) = temp_db();
        let shelf = test_shelf();

        let tx = db.begin_write().unwrap();
        let err = shelf
            .set(
                &tx,
                Key::Int(crate::types::Int(1)),
                Value::String("v".into()),
            )
            .unwrap_err();
        assert!(matches!(err, TransactionError::KeyTypeMismatch { .. }));

        let err = shelf
            .set(
                &tx,
                Key::String("k".into()),
                Value::Int(crate::types::Int(1)),
            )
            .unwrap_err();
        assert!(matches!(err, TransactionError::ValueTypeMismatch { .. }));
    }

    #[test]
    fn test_batch_set_type_validation() {
        let (_file, db) = temp_db();
        let shelf = test_shelf();

        let entries = vec![
            (Key::String("k1".into()), Value::String("v1".into())),
            (Key::Int(crate::types::Int(1)), Value::String("v2".into())),
            (Key::String("k3".into()), Value::Int(crate::types::Int(3))),
        ];

        let tx = db.begin_write().unwrap();
        let results = shelf.batch_set(&tx, &entries).unwrap();

        assert!(results[0].is_ok());
        assert!(matches!(
            results[1],
            Err(TransactionError::KeyTypeMismatch { .. })
        ));
        assert!(matches!(
            results[2],
            Err(TransactionError::ValueTypeMismatch { .. })
        ));
    }

    #[test]
    fn test_batch_put_detects_duplicates() {
        let (_file, db) = temp_db();
        let shelf = test_shelf();

        let entries = vec![
            (Key::String("k1".into()), Value::String("v1".into())),
            (Key::String("k1".into()), Value::String("v2".into())), // duplicate within batch
        ];

        let tx = db.begin_write().unwrap();
        let results = shelf.batch_put(&tx, &entries).unwrap();
        assert!(results[0].is_ok());
        assert!(matches!(
            results[1],
            Err(TransactionError::KeyAlreadyExists)
        ));
    }

    #[test]
    fn test_batch_delete() {
        let (_file, db) = temp_db();
        let shelf = test_shelf();

        // Insert some keys first
        {
            let tx = db.begin_write().unwrap();
            shelf
                .set(&tx, Key::String("a".into()), Value::String("1".into()))
                .unwrap();
            shelf
                .set(&tx, Key::String("b".into()), Value::String("2".into()))
                .unwrap();
            tx.commit().unwrap();
        }

        let keys = vec![
            Key::String("a".into()),        // exists
            Key::String("missing".into()),  // doesn't exist
            Key::Int(crate::types::Int(1)), // wrong type — returns false
        ];

        let tx = db.begin_write().unwrap();
        let results = shelf.batch_delete(&tx, &keys).unwrap();
        tx.commit().unwrap();

        assert!(results[0]); // existed
        assert!(!results[1]); // didn't exist
        assert!(!results[2]); // wrong type, skipped

        // Verify only "b" remains
        let tx = db.begin_read().unwrap();
        assert_eq!(shelf.count(&tx).unwrap(), 1);
        assert_eq!(shelf.get(&tx, &Key::String("a".into())).unwrap(), None);
    }
}
