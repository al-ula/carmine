mod collection;
mod store;

use crate::Number;

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum KeyTypes {
    String,
    BigInt,
    Number,
    Bytes,
}

pub trait KeyType: redb::Key {
    fn key_type(&self) -> KeyTypes;
    fn as_str(&self) -> Option<&str> {
        None
    }
    fn as_int(&self) -> Option<i64> {
        None
    }
    fn as_number(&self) -> Option<Number> {
        None
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl KeyType for String {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::String
    }
    fn as_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl KeyType for i64 {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::BigInt
    }
    fn as_int(&self) -> Option<i64> {
        Some(*self)
    }
}

impl KeyType for Number {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::Number
    }
    fn as_number(&self) -> Option<Number> {
        Some(self.clone())
    }
}

impl KeyType for &[u8] {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::Bytes
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum ValueTypes {
    String,
    BigInt,
    Number,
    Object,
    Bytes,
    Dynamic,
}

pub trait ValueType {
    fn value_type(&self) -> ValueTypes;
    // convert to type that implement redb value trait
    fn to_redb_value(&self) -> impl redb::Value;

    fn as_str(&self) -> Option<&str> {
        None
    }
    fn as_int(&self) -> Option<i64> {
        None
    }
    fn as_number(&self) -> Option<Number> {
        None
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl ValueType for String {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::String
    }

    fn to_redb_value(&self) -> impl redb::Value {
        self.clone()
    }
    fn as_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl ValueType for i64 {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::BigInt
    }

    fn to_redb_value(&self) -> impl redb::Value {
        *self
    }
    fn as_int(&self) -> Option<i64> {
        Some(*self)
    }
}

impl ValueType for Number {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::Number
    }

    fn to_redb_value(&self) -> impl redb::Value {
        self.clone()
    }
    fn as_number(&self) -> Option<Number> {
        Some(self.clone())
    }
}

impl ValueType for &[u8] {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::Bytes
    }

    fn to_redb_value(&self) -> impl redb::Value {
        self.as_ref()
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DynamicValue(pub Vec<u8>);

impl ValueType for DynamicValue {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::Dynamic
    }

    fn to_redb_value(&self) -> impl redb::Value {
        self.0.clone()
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        Some(&self.0)
    }
}

#[derive(PartialEq, Debug, Clone, Eq, Hash)]
pub struct Object(pub Vec<u8>);

impl<'a> From<jsonb::Value<'a>> for Object {
    fn from(value: jsonb::Value) -> Self {
        let bin = value.to_vec();
        return Self(bin);
    }
}

impl ValueType for Object {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::Object
    }

    fn to_redb_value(&self) -> impl redb::Value {
        self.0.clone()
    }
    fn as_bytes(&self) -> Option<&[u8]> {
        Some(&self.0)
    }
}

#[derive(Debug)]
pub enum TableHandle<'txn> {
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

impl<'txn> TableHandle<'txn> {
    pub fn insert<K: KeyType, V: ValueType>(&mut self, key: &K, value: &V) -> crate::Result<()> {
        match self {
            TableHandle::StrStr(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::StrInt(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::StrNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::StrBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_str(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }

            TableHandle::IntStr(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::IntInt(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::IntNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::IntBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_int(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }

            TableHandle::NumberStr(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::NumberInt(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::NumberNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::NumberBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_number(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }

            TableHandle::BytesStr(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_str()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::BytesInt(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_int()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::BytesNumber(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_number()) {
                    table.insert(k, v)?;
                }
            }
            TableHandle::BytesBytes(table) => {
                if let (Some(k), Some(v)) = (key.as_bytes(), value.as_bytes()) {
                    table.insert(k, v)?;
                }
            }
        }
        Ok(())
    }
    pub fn remove<K: KeyType>(&mut self, key: &K) -> crate::Result<()> {
        match self {
            TableHandle::StrStr(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            TableHandle::StrInt(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            TableHandle::StrNumber(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            TableHandle::StrBytes(table) => {
                if let Some(k) = key.as_str() {
                    table.remove(k)?;
                }
            }
            TableHandle::IntStr(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            TableHandle::IntInt(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            TableHandle::IntNumber(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            TableHandle::IntBytes(table) => {
                if let Some(k) = key.as_int() {
                    table.remove(k)?;
                }
            }
            TableHandle::NumberStr(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            TableHandle::NumberInt(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            TableHandle::NumberNumber(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            TableHandle::NumberBytes(table) => {
                if let Some(k) = key.as_number() {
                    table.remove(k)?;
                }
            }
            TableHandle::BytesStr(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
            TableHandle::BytesInt(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
            TableHandle::BytesNumber(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
            TableHandle::BytesBytes(table) => {
                if let Some(k) = key.as_bytes() {
                    table.remove(k)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ReadOnlyTableHandle {
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

#[cfg(test)]
mod tests;
