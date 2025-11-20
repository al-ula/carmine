use redb::TableDefinition;

use crate::{
    Number,
    database::{
        KeyType, KeyTypes, ReadOnlyTableHandle, TableHandle, ValueType, ValueTypes, store::Store,
    },
    error::Result,
    validation::validate_collection_name,
};

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
    pub fn write<'txn>(&self, txn: &'txn redb::WriteTransaction) -> Result<TableHandle<'txn>> {
        match (self.key_type, self.value_type) {
            (KeyTypes::String, ValueTypes::String) => {
                let table = TableDefinition::<&str, &str>::new(&self.name);
                Ok(TableHandle::StrStr(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::BigInt) => {
                let table = TableDefinition::<&str, i64>::new(&self.name);
                Ok(TableHandle::StrInt(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Number) => {
                let table = TableDefinition::<&str, Number>::new(&self.name);
                Ok(TableHandle::StrNumber(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Bytes) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(TableHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Object) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(TableHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(TableHandle::StrBytes(txn.open_table(table)?))
            }

            (KeyTypes::BigInt, ValueTypes::String) => {
                let table = TableDefinition::<i64, &str>::new(&self.name);
                Ok(TableHandle::IntStr(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::BigInt) => {
                let table = TableDefinition::<i64, i64>::new(&self.name);
                Ok(TableHandle::IntInt(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Number) => {
                let table = TableDefinition::<i64, Number>::new(&self.name);
                Ok(TableHandle::IntNumber(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Bytes) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(TableHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Object) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(TableHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Dynamic) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(TableHandle::IntBytes(txn.open_table(table)?))
            }

            (KeyTypes::Number, ValueTypes::String) => {
                let table = TableDefinition::<Number, &str>::new(&self.name);
                Ok(TableHandle::NumberStr(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::BigInt) => {
                let table = TableDefinition::<Number, i64>::new(&self.name);
                Ok(TableHandle::NumberInt(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Number) => {
                let table = TableDefinition::<Number, Number>::new(&self.name);
                Ok(TableHandle::NumberNumber(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Bytes) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(TableHandle::NumberBytes(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Object) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(TableHandle::NumberBytes(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Dynamic) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(TableHandle::NumberBytes(txn.open_table(table)?))
            }

            (KeyTypes::Bytes, ValueTypes::String) => {
                let table = TableDefinition::<&[u8], &str>::new(&self.name);
                Ok(TableHandle::BytesStr(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::BigInt) => {
                let table = TableDefinition::<&[u8], i64>::new(&self.name);
                Ok(TableHandle::BytesInt(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Number) => {
                let table = TableDefinition::<&[u8], Number>::new(&self.name);
                Ok(TableHandle::BytesNumber(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Bytes) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(TableHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Object) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(TableHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(TableHandle::BytesBytes(txn.open_table(table)?))
            }
        }
    }

    pub fn read<'txn>(&self, txn: &'txn redb::ReadTransaction) -> Result<ReadOnlyTableHandle> {
        match (self.key_type, self.value_type) {
            (KeyTypes::String, ValueTypes::String) => {
                let table = TableDefinition::<&str, &str>::new(&self.name);
                Ok(ReadOnlyTableHandle::StrStr(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::BigInt) => {
                let table = TableDefinition::<&str, i64>::new(&self.name);
                Ok(ReadOnlyTableHandle::StrInt(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Number) => {
                let table = TableDefinition::<&str, Number>::new(&self.name);
                Ok(ReadOnlyTableHandle::StrNumber(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Bytes) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Object) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::StrBytes(txn.open_table(table)?))
            }
            (KeyTypes::String, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&str, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::StrBytes(txn.open_table(table)?))
            }

            (KeyTypes::BigInt, ValueTypes::String) => {
                let table = TableDefinition::<i64, &str>::new(&self.name);
                Ok(ReadOnlyTableHandle::IntStr(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::BigInt) => {
                let table = TableDefinition::<i64, i64>::new(&self.name);
                Ok(ReadOnlyTableHandle::IntInt(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Number) => {
                let table = TableDefinition::<i64, Number>::new(&self.name);
                Ok(ReadOnlyTableHandle::IntNumber(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Bytes) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Object) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::IntBytes(txn.open_table(table)?))
            }
            (KeyTypes::BigInt, ValueTypes::Dynamic) => {
                let table = TableDefinition::<i64, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::IntBytes(txn.open_table(table)?))
            }

            (KeyTypes::Number, ValueTypes::String) => {
                let table = TableDefinition::<Number, &str>::new(&self.name);
                Ok(ReadOnlyTableHandle::NumberStr(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::BigInt) => {
                let table = TableDefinition::<Number, i64>::new(&self.name);
                Ok(ReadOnlyTableHandle::NumberInt(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Number) => {
                let table = TableDefinition::<Number, Number>::new(&self.name);
                Ok(ReadOnlyTableHandle::NumberNumber(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Bytes) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::NumberBytes(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Object) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::NumberBytes(txn.open_table(table)?))
            }
            (KeyTypes::Number, ValueTypes::Dynamic) => {
                let table = TableDefinition::<Number, &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::NumberBytes(txn.open_table(table)?))
            }

            (KeyTypes::Bytes, ValueTypes::String) => {
                let table = TableDefinition::<&[u8], &str>::new(&self.name);
                Ok(ReadOnlyTableHandle::BytesStr(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::BigInt) => {
                let table = TableDefinition::<&[u8], i64>::new(&self.name);
                Ok(ReadOnlyTableHandle::BytesInt(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Number) => {
                let table = TableDefinition::<&[u8], Number>::new(&self.name);
                Ok(ReadOnlyTableHandle::BytesNumber(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Bytes) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Object) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::BytesBytes(txn.open_table(table)?))
            }
            (KeyTypes::Bytes, ValueTypes::Dynamic) => {
                let table = TableDefinition::<&[u8], &[u8]>::new(&self.name);
                Ok(ReadOnlyTableHandle::BytesBytes(txn.open_table(table)?))
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
