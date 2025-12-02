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

    fn as_str(&self) -> Option<&str> {
        Some(self)
    }
}

impl ValueType for i64 {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::BigInt
    }

    fn as_int(&self) -> Option<i64> {
        Some(*self)
    }
}

impl ValueType for Number {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::Number
    }

    fn as_number(&self) -> Option<Number> {
        Some(self.clone())
    }
}

impl ValueType for &[u8] {
    fn value_type(&self) -> ValueTypes {
        ValueTypes::Bytes
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl ValueType for Vec<u8> {
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
        Some(self.as_slice())
    }

    fn value_type(&self) -> ValueTypes {
        ValueTypes::Bytes
    }
}
