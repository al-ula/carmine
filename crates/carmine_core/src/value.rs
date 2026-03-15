use thiserror::Error;

use crate::{
    key::Key,
    types::{Int, Number, RawObject},
};

pub type ResultString = std::result::Result<Option<String>, BatchItemError>;
pub type ResultNumber = std::result::Result<Option<Number>, BatchItemError>;
pub type ResultInt = std::result::Result<Option<Int>, BatchItemError>;
pub type ResultObject = std::result::Result<Option<RawObject>, BatchItemError>;
pub type ResultByte = std::result::Result<Option<Vec<u8>>, BatchItemError>;

#[derive(Debug, Error)]
pub enum ValueError {
    #[error("Invalid conversion from value")]
    InvalidConversion,
    #[error("Value is not a valid key type")]
    InvalidKeyType,
}

#[derive(Debug, Error, Clone)]
pub enum BatchItemError {
    #[error("Key type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

#[derive(Debug, Hash, Clone, PartialEq)]
pub enum Value {
    String(String),
    Number(Number),
    Int(Int),
    Object(RawObject),
    Byte(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
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
    String(Vec<ResultString>),
    Number(Vec<ResultNumber>),
    Int(Vec<ResultInt>),
    Object(Vec<ResultObject>),
    Byte(Vec<ResultByte>),
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
            ValueType::String => ValueRetVec::String(vec![Ok(None); size]),
            ValueType::Number => ValueRetVec::Number(vec![Ok(None); size]),
            ValueType::Int => ValueRetVec::Int(vec![Ok(None); size]),
            ValueType::Object => ValueRetVec::Object(vec![Ok(None); size]),
            ValueType::Byte => ValueRetVec::Byte(vec![Ok(None); size]),
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

    pub fn get(&self, index: usize) -> Result<Option<Value>, BatchItemError> {
        match self {
            ValueRetVec::String(s) => {
                let res = s.get(index).ok_or_else(|| BatchItemError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: "index out of bounds".to_string(),
                })?;
                match res {
                    Ok(opt) => Ok(opt.as_ref().map(|v| Value::String(v.clone()))),
                    Err(e) => Err(e.clone()),
                }
            }
            ValueRetVec::Number(n) => {
                let res = n.get(index).ok_or_else(|| BatchItemError::TypeMismatch {
                    expected: "Number".to_string(),
                    actual: "index out of bounds".to_string(),
                })?;
                match res {
                    Ok(opt) => Ok(opt.as_ref().map(|v| Value::Number(v.clone()))),
                    Err(e) => Err(e.clone()),
                }
            }
            ValueRetVec::Int(i) => {
                let res = i.get(index).ok_or_else(|| BatchItemError::TypeMismatch {
                    expected: "Int".to_string(),
                    actual: "index out of bounds".to_string(),
                })?;
                match res {
                    Ok(opt) => Ok(opt.as_ref().map(|v| Value::Int(*v))),
                    Err(e) => Err(e.clone()),
                }
            }
            ValueRetVec::Object(o) => {
                let res = o.get(index).ok_or_else(|| BatchItemError::TypeMismatch {
                    expected: "Object".to_string(),
                    actual: "index out of bounds".to_string(),
                })?;
                match res {
                    Ok(opt) => Ok(opt.as_ref().map(|v| Value::Object(v.clone()))),
                    Err(e) => Err(e.clone()),
                }
            }
            ValueRetVec::Byte(b) => {
                let res = b.get(index).ok_or_else(|| BatchItemError::TypeMismatch {
                    expected: "Byte".to_string(),
                    actual: "index out of bounds".to_string(),
                })?;
                match res {
                    Ok(opt) => Ok(opt.as_ref().map(|v| Value::Byte(v.clone()))),
                    Err(e) => Err(e.clone()),
                }
            }
        }
    }
    pub fn set(
        &mut self,
        index: usize,
        value: Result<Option<Value>, BatchItemError>,
    ) -> Result<(), ValueError> {
        match (self, value) {
            (ValueRetVec::String(s), Ok(Some(Value::String(v)))) => {
                if index < s.len() {
                    s[index] = Ok(Some(v));
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Number(n), Ok(Some(Value::Number(v)))) => {
                if index < n.len() {
                    n[index] = Ok(Some(v));
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Int(i), Ok(Some(Value::Int(v)))) => {
                if index < i.len() {
                    i[index] = Ok(Some(v));
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Object(o), Ok(Some(Value::Object(v)))) => {
                if index < o.len() {
                    o[index] = Ok(Some(v));
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Byte(b), Ok(Some(Value::Byte(v)))) => {
                if index < b.len() {
                    b[index] = Ok(Some(v));
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::String(s), Ok(None)) => {
                if index < s.len() {
                    s[index] = Ok(None);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Number(n), Ok(None)) => {
                if index < n.len() {
                    n[index] = Ok(None);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Int(i), Ok(None)) => {
                if index < i.len() {
                    i[index] = Ok(None);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Object(o), Ok(None)) => {
                if index < o.len() {
                    o[index] = Ok(None);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (ValueRetVec::Byte(b), Ok(None)) => {
                if index < b.len() {
                    b[index] = Ok(None);
                    Ok(())
                } else {
                    Err(ValueError::InvalidConversion)
                }
            }
            (v, Err(e)) => {
                let idx = match v {
                    ValueRetVec::String(s) => {
                        if index < s.len() {
                            s[index] = Err(e.clone());
                            return Ok(());
                        } else {
                            return Err(ValueError::InvalidConversion);
                        }
                    }
                    ValueRetVec::Number(n) => {
                        if index < n.len() {
                            n[index] = Err(e.clone());
                            return Ok(());
                        } else {
                            return Err(ValueError::InvalidConversion);
                        }
                    }
                    ValueRetVec::Int(i) => {
                        if index < i.len() {
                            i[index] = Err(e.clone());
                            return Ok(());
                        } else {
                            return Err(ValueError::InvalidConversion);
                        }
                    }
                    ValueRetVec::Object(o) => {
                        if index < o.len() {
                            o[index] = Err(e.clone());
                            return Ok(());
                        } else {
                            return Err(ValueError::InvalidConversion);
                        }
                    }
                    ValueRetVec::Byte(b) => {
                        if index < b.len() {
                            b[index] = Err(e.clone());
                            return Ok(());
                        } else {
                            return Err(ValueError::InvalidConversion);
                        }
                    }
                };
            }
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl TryFrom<ValueRetVec> for Vec<Result<Option<String>, BatchItemError>> {
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
        ValueRetVec::String(vec.into_iter().map(Ok).collect())
    }
}

impl TryFrom<ValueRetVec> for Vec<std::result::Result<Option<Number>, BatchItemError>> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> std::result::Result<Self, Self::Error> {
        match value {
            ValueRetVec::Number(n) => Ok(n),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<Number>>> for ValueRetVec {
    fn from(vec: Vec<Option<Number>>) -> Self {
        ValueRetVec::Number(vec.into_iter().map(Ok).collect())
    }
}

impl TryFrom<ValueRetVec> for Vec<ResultInt> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> std::result::Result<Self, Self::Error> {
        match value {
            ValueRetVec::Int(i) => Ok(i),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<Int>>> for ValueRetVec {
    fn from(vec: Vec<Option<Int>>) -> Self {
        ValueRetVec::Int(vec.into_iter().map(Ok).collect())
    }
}

impl TryFrom<ValueRetVec> for Vec<ResultObject> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> std::result::Result<Self, Self::Error> {
        match value {
            ValueRetVec::Object(o) => Ok(o),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<RawObject>>> for ValueRetVec {
    fn from(vec: Vec<Option<RawObject>>) -> Self {
        ValueRetVec::Object(vec.into_iter().map(Ok).collect())
    }
}

impl TryFrom<ValueRetVec> for Vec<ResultByte> {
    type Error = ValueError;

    fn try_from(value: ValueRetVec) -> std::result::Result<Self, Self::Error> {
        match value {
            ValueRetVec::Byte(b) => Ok(b),
            _ => Err(ValueError::InvalidConversion),
        }
    }
}

impl From<Vec<Option<Vec<u8>>>> for ValueRetVec {
    fn from(vec: Vec<Option<Vec<u8>>>) -> Self {
        ValueRetVec::Byte(vec.into_iter().map(Ok).collect())
    }
}
