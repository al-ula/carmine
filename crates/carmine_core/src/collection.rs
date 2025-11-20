use redb::TableDefinition;

use crate::{
    error::Result,
    validation::validate_collection_name,
    Number,
};
use crate::key::{KeyType, KeyTypes};
use crate::value::{ValueType, ValueTypes};
use crate::store::Store;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Collection {
    pub name: String,
    pub key_type: KeyTypes,
    pub value_type: ValueTypes,
}

impl Collection {
    pub fn create(name: String, key_type: KeyTypes, value_type: ValueTypes) -> Result<Self> {
        validate_collection_name(&name)?;
        Ok(Self {
            name,
            key_type,
            value_type,
        })
    }
    pub fn rename(&mut self, name: String) -> Result<()> {
        validate_collection_name(&name)?;
        self.name = name;
        Ok(())
    }
    pub fn write<'txn>(&self, txn: &'txn redb::WriteTransaction) -> Result<CollectionHandle<'txn>> {
        match (self.key_type, self.value_type) {
            (KeyTypes::String, ValueTypes::String) => {
                let table = TableDefinition::<&str, &str>::new(&self.name);
                Ok(CollectionHandle::StrStr(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::BigInt) => {
                let table = TableDefinition::<&str, i64>::new(&self.name);
                Ok(CollectionHandle::StrInt(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Number) => {
                let table = TableDefinition::<&str, Number>::new(&self.name);
                Ok(CollectionHandle::StrNumber(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Bytes) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(CollectionHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Object) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(CollectionHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(CollectionHandle::StrBytes(txn.open_table(table)?))
            }

            (KeyTypes::BigInt, ValueTypes::String) => {
                let table = TableDefinition::<i64, &str>::new(&self.name);
                Ok(CollectionHandle::IntStr(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::BigInt) => {
                let table = TableDefinition::<i64, i64>::new(&self.name);
                Ok(CollectionHandle::IntInt(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Number) => {
                let table = TableDefinition::<i64, Number>::new(&self.name);
                Ok(CollectionHandle::IntNumber(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Bytes) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(CollectionHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Object) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(CollectionHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Dynamic) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(CollectionHandle::IntBytes(txn.open_table(table)?))
            }

            (KeyTypes::Number, ValueTypes::String) => {
                let table = TableDefinition::<Number, &str>::new(&self.name);
                Ok(CollectionHandle::NumberStr(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::BigInt) => {
                let table = TableDefinition::<Number, i64>::new(&self.name);
                Ok(CollectionHandle::NumberInt(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Number) => {
                let table = TableDefinition::<Number, Number>::new(&self.name);
                Ok(CollectionHandle::NumberNumber(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Bytes) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(CollectionHandle::NumberBytes(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Object) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(CollectionHandle::NumberBytes(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Dynamic) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(CollectionHandle::NumberBytes(txn.open_table(table)?))
            }

            (KeyTypes::Bytes, ValueTypes::String) => {
                let table = TableDefinition::<&[u8], &str>::new(&self.name);
                Ok(CollectionHandle::BytesStr(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::BigInt) => {
                let table = TableDefinition::<&[u8], i64>::new(&self.name);
                Ok(CollectionHandle::BytesInt(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Number) => {
                let table = TableDefinition::<&[u8], Number>::new(&self.name);
                Ok(CollectionHandle::BytesNumber(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Bytes) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(CollectionHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Object) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(CollectionHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(CollectionHandle::BytesBytes(txn.open_table(table)?))
            }
        }
    }

    pub fn read<'txn>(&self, txn: &'txn redb::ReadTransaction) -> Result<ReadOnlyCollectionHandle> {
        match (self.key_type, self.value_type) {
            (KeyTypes::String, ValueTypes::String) => {
                let table = TableDefinition::<&str, &str>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::StrStr(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::BigInt) => {
                let table = TableDefinition::<&str, i64>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::StrInt(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Number) => {
                let table = TableDefinition::<&str, Number>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::StrNumber(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Bytes) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Object) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::StrBytes(txn.open_table(table)?))
            }

            (KeyTypes::BigInt, ValueTypes::String) => {
                let table = TableDefinition::<i64, &str>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::IntStr(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::BigInt) => {
                let table = TableDefinition::<i64, i64>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::IntInt(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Number) => {
                let table = TableDefinition::<i64, Number>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::IntNumber(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Bytes) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Object) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Dynamic) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::IntBytes(txn.open_table(table)?))
            }

            (KeyTypes::Number, ValueTypes::String) => {
                let table = TableDefinition::<Number, &str>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::NumberStr(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::BigInt) => {
                let table = TableDefinition::<Number, i64>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::NumberInt(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Number) => {
                let table = TableDefinition::<Number, Number>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::NumberNumber(
                    txn.open_table(table)?,
                ))
            }
            (KeyTypes::Number, ValueTypes::Bytes) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::NumberBytes(
                    txn.open_table(table)?,
                ))
            }
            (KeyTypes::Number, ValueTypes::Object) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::NumberBytes(
                    txn.open_table(table)?,
                ))
            }
            (KeyTypes::Number, ValueTypes::Dynamic) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::NumberBytes(
                    txn.open_table(table)?,
                ))
            }

            (KeyTypes::Bytes, ValueTypes::String) => {
                let table = TableDefinition::<&[u8], &str>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::BytesStr(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::BigInt) => {
                let table = TableDefinition::<&[u8], i64>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::BytesInt(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Number) => {
                let table = TableDefinition::<&[u8], Number>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::BytesNumber(
                    txn.open_table(table)?,
                ))
            }
            (KeyTypes::Bytes, ValueTypes::Bytes) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Object) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(ReadOnlyCollectionHandle::BytesBytes(txn.open_table(table)?))
            }
        }
    }

    pub fn write_one<'txn, T, U>(&self, key: T, value: U, store: &Store) -> Result<()>
    where
        T: KeyType,
        U: ValueType,
    {
        let txn = store.handle.begin_write()?;
        {
            let mut table = self.write(&txn)?;
            table.insert(&key, &value)?;
        }
        txn.commit()?;
        Ok(())
    }
    pub fn write_many<'txn, T, U>(&self, keys: Vec<T>, values: Vec<U>, store: &Store) -> Result<()>
    where
        T: KeyType,
        U: ValueType,
    {
        let txn = store.handle.begin_write()?;
        {
            let mut table = self.write(&txn)?;
            for (key, value) in keys.iter().zip(values.iter()) {
                table.insert(key, value)?;
            }
        }
        txn.commit()?;
        Ok(())
    }
    pub fn delete_one<'txn, T>(&self, key: T, store: &Store) -> Result<()>
    where
        T: KeyType,
    {
        let txn = store.handle.begin_write()?;
        {
            let mut table = self.write(&txn)?;
            table.remove(&key)?;
        }
        txn.commit()?;
        Ok(())
    }
    pub fn delete_many<'txn, T>(&self, keys: Vec<T>, store: &Store) -> Result<()>
    where
        T: KeyType,
    {
        let txn = store.handle.begin_write()?;
        {
            let mut table = self.write(&txn)?;
            for key in keys.iter() {
                table.remove(key)?;
            }
        }
        txn.commit()?;
        Ok(())
    }
    // pub fn delete_all(&self, store: &Store) -> Result<()> {
    //     let txn = store.handle.begin_write()?;
    //     {
    //         let mut table = self.write(&txn)?;
    //         table.clear()?;
    //     }
    //     txn.commit()?;
    //     Ok(())
    // }
}

#[derive(Debug)]
pub enum CollectionHandle<'txn> {
    StrStr(redb::Table<'txn, &'static str, &'static str>),
    StrInt(redb::Table<'txn, &'static str, i64>),
    StrNumber(redb::Table<'txn, &'static str, Number>),
    StrBytes(redb::Table<'txn, &'static str, &'static [u8]>),

    IntStr(redb::Table<'txn, i64, &'static str>),
    IntInt(redb::Table<'txn, i64, i64>),
    IntNumber(redb::Table<'txn, i64, Number>),
    IntBytes(redb::Table<'txn, i64, &'static [u8]>),

    NumberStr(redb::Table<'txn, Number, &'static str>),
    NumberInt(redb::Table<'txn, Number, i64>),
    NumberNumber(redb::Table<'txn, Number, Number>),
    NumberBytes(redb::Table<'txn, Number, &'static [u8]>),

    BytesStr(redb::Table<'txn, &'static [u8], &'static str>),
    BytesInt(redb::Table<'txn, &'static [u8], i64>),
    BytesNumber(redb::Table<'txn, &'static [u8], Number>),
    BytesBytes(redb::Table<'txn, &'static [u8], &'static [u8]>),
}

impl<'txn> CollectionHandle<'txn> {
    pub fn insert<K: KeyType, V: ValueType>(&mut self, key: &K, value: &V) -> crate::Result<()> {
        match self {
            CollectionHandle::StrStr(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::StrInt(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::StrNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::StrBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }

            CollectionHandle::IntStr(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::IntInt(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::IntNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::IntBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }

            CollectionHandle::NumberStr(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::NumberInt(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::NumberNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::NumberBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }

            CollectionHandle::BytesStr(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::BytesInt(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::BytesNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            CollectionHandle::BytesBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }
        }
        Ok(())
    }
    pub fn remove<K: KeyType>(&mut self, key: &K) -> crate::Result<()> {
        match self {
            CollectionHandle::StrStr(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::StrInt(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::StrNumber(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::StrBytes(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::IntStr(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::IntInt(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::IntNumber(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::IntBytes(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::NumberStr(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::NumberInt(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::NumberNumber(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::NumberBytes(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::BytesStr(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::BytesInt(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::BytesNumber(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
            CollectionHandle::BytesBytes(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ReadOnlyCollectionHandle {
    StrStr(redb::ReadOnlyTable<&'static str, &'static str>),
    StrInt(redb::ReadOnlyTable<&'static str, i64>),
    StrNumber(redb::ReadOnlyTable<&'static str, Number>),
    StrBytes(redb::ReadOnlyTable<&'static str, &'static [u8]>),

    IntStr(redb::ReadOnlyTable<i64, &'static str>),
    IntInt(redb::ReadOnlyTable<i64, i64>),
    IntNumber(redb::ReadOnlyTable<i64, Number>),
    IntBytes(redb::ReadOnlyTable<i64, &'static [u8]>),

    NumberStr(redb::ReadOnlyTable<Number, &'static str>),
    NumberInt(redb::ReadOnlyTable<Number, i64>),
    NumberNumber(redb::ReadOnlyTable<Number, Number>),
    NumberBytes(redb::ReadOnlyTable<Number, &'static [u8]>),

    BytesStr(redb::ReadOnlyTable<&'static [u8], &'static str>),
    BytesInt(redb::ReadOnlyTable<&'static [u8], i64>),
    BytesNumber(redb::ReadOnlyTable<&'static [u8], Number>),
    BytesBytes(redb::ReadOnlyTable<&'static [u8], &'static [u8]>),
}