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