use crate::{types::Int, types::Number, value::Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Error)]
pub enum KeyError {
    #[error("Key is not a string")]
    NotAString,
    #[error("Key is not a number")]
    NotANumber,
    #[error("Key is not an int")]
    NotAnInt,
}

#[derive(Debug, Hash, Clone, Serialize, Deserialize)]
pub enum Key {
    String(String),
    Number(Number),
    Int(Int),
}

#[derive(Debug, Clone, Copy)]
pub enum KeyType {
    String,
    Number,
    Int,
}

impl Key {
    pub fn as_type(&self) -> KeyType {
        match self {
            Key::String(_) => KeyType::String,
            Key::Number(_) => KeyType::Number,
            Key::Int(_) => KeyType::Int,
        }
    }
}

impl TryFrom<Key> for String {
    type Error = KeyError;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        match key {
            Key::String(s) => Ok(s),
            _ => Err(KeyError::NotAString),
        }
    }
}

impl TryFrom<Key> for Number {
    type Error = KeyError;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        match key {
            Key::Number(n) => Ok(n),
            _ => Err(KeyError::NotANumber),
        }
    }
}

impl TryFrom<Key> for Int {
    type Error = KeyError;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        match key {
            Key::Int(i) => Ok(i),
            _ => Err(KeyError::NotAnInt),
        }
    }
}

impl From<Key> for Value {
    fn from(key: Key) -> Self {
        match key {
            Key::String(s) => Value::String(s),
            Key::Number(n) => Value::Number(n),
            Key::Int(i) => Value::Int(i),
        }
    }
}
impl<T: ToString> From<T> for Key {
    fn from(key: T) -> Self {
        Key::String(key.to_string())
    }
}
