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

#[derive(Debug, Hash, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Number(Number),
    Int(Int),
    Object(RawObject),
    Byte(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    String,
    Number,
    Int,
    Object,
    Byte,
}

impl Value {
    pub fn as_type(&self) -> ValueType {
        match self {
            Value::String(_) => ValueType::String,
            Value::Number(_) => ValueType::Number,
            Value::Int(_) => ValueType::Int,
            Value::Object(_) => ValueType::Object,
            Value::Byte(_) => ValueType::Byte,
        }
    }
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

impl From<String> for Value {
    fn from(num: String) -> Self {
        Value::String(num)
    }
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

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Self {
        Value::Byte(bytes)
    }
}

impl From<RawObject> for Value {
    fn from(object: RawObject) -> Self {
        Value::Object(object)
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

#[derive(Debug, Clone)]
pub enum ValueRetVec {
    String(Vec<Option<String>>),
    Number(Vec<Option<Number>>),
    Int(Vec<Option<Int>>),
    Object(Vec<Option<RawObject>>),
    Byte(Vec<Option<Vec<u8>>>),
}

impl ValueRetVec {
    pub fn as_type(&self) -> ValueType {
        match self {
            ValueRetVec::String(_) => ValueType::String,
            ValueRetVec::Number(_) => ValueType::Number,
            ValueRetVec::Int(_) => ValueType::Int,
            ValueRetVec::Object(_) => ValueType::Object,
            ValueRetVec::Byte(_) => ValueType::Byte,
        }
    }
    pub fn new(value_type: ValueType, size: usize) -> Self {
        match value_type {
            ValueType::String => ValueRetVec::String(vec![None; size]),
            ValueType::Number => ValueRetVec::Number(vec![None; size]),
            ValueType::Int => ValueRetVec::Int(vec![None; size]),
            ValueType::Object => ValueRetVec::Object(vec![None; size]),
            ValueType::Byte => ValueRetVec::Byte(vec![None; size]),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            ValueRetVec::String(s) => s.len(),
            ValueRetVec::Number(n) => n.len(),
            ValueRetVec::Int(i) => i.len(),
            ValueRetVec::Object(o) => o.len(),
            ValueRetVec::Byte(b) => b.len(),
        }
    }

    pub fn get(&self, index: usize) -> Option<Value> {
        match self {
            ValueRetVec::String(s) => s
                .get(index)
                .and_then(|opt| opt.as_ref().map(|v| Value::String(v.clone()))),
            ValueRetVec::Number(n) => n
                .get(index)
                .and_then(|opt| opt.as_ref().map(|v| Value::Number(v.clone()))),
            ValueRetVec::Int(i) => i
                .get(index)
                .and_then(|opt| opt.as_ref().map(|v| Value::Int(*v))),
            ValueRetVec::Object(o) => o
                .get(index)
                .and_then(|opt| opt.as_ref().map(|v| Value::Object(v.clone()))),
            ValueRetVec::Byte(b) => b
                .get(index)
                .and_then(|opt| opt.as_ref().map(|v| Value::Byte(v.clone()))),
        }
    }
    pub fn set(&mut self, index: usize, value: Option<Value>) -> Result<(), ValueError> {
        match (self, value) {
            (ValueRetVec::String(s), Some(Value::String(v))) => {
                if index < s.len() {
                    s[index] = Some(v);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Number(n), Some(Value::Number(v))) => {
                if index < n.len() {
                    n[index] = Some(v);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Int(i), Some(Value::Int(v))) => {
                if index < i.len() {
                    i[index] = Some(v);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Object(o), Some(Value::Object(v))) => {
                if index < o.len() {
                    o[index] = Some(v);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Byte(b), Some(Value::Byte(v))) => {
                if index < b.len() {
                    b[index] = Some(v);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl TryFrom<ValueRetVec> for Vec<Option<String>> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> Result<Self, Self::Error> {
        match value {
            ValueRetVec::String(s) => Ok(s),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<String>>> for ValueRetVec {
    fn from(vec: Vec<Option<String>>) -> Self {
        ValueRetVec::String(vec)
    }
}

impl TryFrom<ValueRetVec> for Vec<Option<Number>> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> Result<Self, Self::Error> {
        match value {
            ValueRetVec::Number(n) => Ok(n),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<Number>>> for ValueRetVec {
    fn from(vec: Vec<Option<Number>>) -> Self {
        ValueRetVec::Number(vec)
    }
}

impl TryFrom<ValueRetVec> for Vec<Option<Int>> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> Result<Self, Self::Error> {
        match value {
            ValueRetVec::Int(i) => Ok(i),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<Int>>> for ValueRetVec {
    fn from(vec: Vec<Option<Int>>) -> Self {
        ValueRetVec::Int(vec)
    }
}

impl TryFrom<ValueRetVec> for Vec<Option<RawObject>> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> Result<Self, Self::Error> {
        match value {
            ValueRetVec::Object(o) => Ok(o),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<RawObject>>> for ValueRetVec {
    fn from(vec: Vec<Option<RawObject>>) -> Self {
        ValueRetVec::Object(vec)
    }
}

impl TryFrom<ValueRetVec> for Vec<Option<Vec<u8>>> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> Result<Self, Self::Error> {
        match value {
            ValueRetVec::Byte(b) => Ok(b),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<Vec<u8>>>> for ValueRetVec {
    fn from(vec: Vec<Option<Vec<u8>>>) -> Self {
        ValueRetVec::Byte(vec)
    }
}
