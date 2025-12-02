use crate::{Number, error::Error, error::Result};

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum KeyTypes {
    String,
    BigInt,
    Number,
    Bytes,
}

pub trait KeyType: redb::Key {
    fn key_type(&self) -> KeyTypes;
    fn as_str(&self) -> Result<&str> {
        Err(Error::KeyTypeMismatch {
            e: KeyTypes::String,
            g: self.key_type(),
        })
    }
    fn as_int(&self) -> Result<i64> {
        Err(Error::KeyTypeMismatch {
            e: KeyTypes::BigInt,
            g: self.key_type(),
        })
    }
    fn as_number(&self) -> Result<Number> {
        Err(Error::KeyTypeMismatch {
            e: KeyTypes::Number,
            g: self.key_type(),
        })
    }
    fn as_bytes(&self) -> Result<&[u8]> {
        Err(Error::KeyTypeMismatch {
            e: KeyTypes::Bytes,
            g: self.key_type(),
        })
    }
}

impl KeyType for String {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::String
    }
    fn as_str(&self) -> Result<&str> {
        Ok(self)
    }
}

impl KeyType for i64 {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::BigInt
    }
    fn as_int(&self) -> Result<i64> {
        Ok(*self)
    }
}

impl KeyType for Number {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::Number
    }
    fn as_number(&self) -> Result<Number> {
        Ok(self.clone())
    }
}

impl KeyType for &[u8] {
    fn key_type(&self) -> KeyTypes {
        KeyTypes::Bytes
    }
    fn as_bytes(&self) -> Result<&[u8]> {
        Ok(self)
    }
}
