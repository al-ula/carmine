use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    key::Key,
    types::{Int, Number, RawObject},
};

#[derive(Debug, Error)]
pub enum ValueError {
    #[error("Invalid conversion from value")]
    InvalidConversion,
    #[error("Value is not a valid key type")]
    InvalidKeyType,
}

impl From<Number> for Value {
    fn from(num: Number) -> Self {
        Value::Number(num)
    }
}

impl From<Int> for Value {
    fn from(num: Int) -> Self {
        Value::Int(num)
    }
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Number(Number),
    Int(Int),
    Object(RawObject),
    Byte(Vec<u8>),
}

impl TryFrom<Value> for Key {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(Key::String(s)),
            Value::Number(n) => Ok(Key::Number(n)),
            Value::Int(i) => Ok(Key::Int(i)),
            _ => Err(ValueError::InvalidKeyType),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(s),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl TryFrom<Value> for Number {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Number(n) => Ok(n),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl TryFrom<Value> for Int {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(i),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Byte(b) => Ok(b),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl TryFrom<Value> for RawObject {
    type Error = ValueError;
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Object(n) => Ok(n),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}
