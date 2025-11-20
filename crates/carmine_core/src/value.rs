use crate::Number;

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