use jsonb::{self, from_raw_jsonb, RawJsonb};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::{
    convert::TryInto,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
};

#[derive(Debug, Clone, Copy, Error)]
pub enum TypesError {
    #[error("Failed to deserialize number from jsonb bytes")]
    NumberDeserialize,
    #[error("Invalid 8-byte slice for int conversion")]
    InvalidIntBytes,
    #[error("Number comparison failed")]
    NumberCompare,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RawObject(pub(crate) Vec<u8>);
impl Deref for RawObject {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RawObject {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Number(pub(crate) jsonb::Number);
impl Deref for Number {
    type Target = jsonb::Number;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Number {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Hash for Number {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        std::mem::discriminant(&self.0).hash(hasher);
        match &self.0 {
            jsonb::Number::Int64(v) => v.hash(hasher),
            jsonb::Number::UInt64(v) => v.hash(hasher),
            jsonb::Number::Float64(v) => v.to_bits().hash(hasher),
            jsonb::Number::Decimal64(v) => {
                v.scale.hash(hasher);
                v.value.hash(hasher);
            }
            jsonb::Number::Decimal128(v) => {
                v.scale.hash(hasher);
                v.value.hash(hasher);
            }
            jsonb::Number::Decimal256(v) => {
                v.scale.hash(hasher);
                let (hi, lo) = v.value.into_words();
                hi.hash(hasher);
                lo.hash(hasher);
            }
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (jsonb::Number::Int64(a), jsonb::Number::Int64(b)) => a == b,
            (jsonb::Number::UInt64(a), jsonb::Number::UInt64(b)) => a == b,
            (jsonb::Number::Float64(a), jsonb::Number::Float64(b)) => a.to_bits() == b.to_bits(),
            (jsonb::Number::Decimal64(a), jsonb::Number::Decimal64(b)) => {
                a.scale == b.scale && a.value == b.value
            }
            (jsonb::Number::Decimal128(a), jsonb::Number::Decimal128(b)) => {
                a.scale == b.scale && a.value == b.value
            }
            (jsonb::Number::Decimal256(a), jsonb::Number::Decimal256(b)) => {
                a.scale == b.scale && a.value == b.value
            }
            _ => false,
        }
    }
}

impl Eq for Number {}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Int(pub(crate) i64);
impl Deref for Int {
    type Target = i64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Int {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl redb::Value for Number {
    type SelfType<'a>
        = Number
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let raw_jsonb = RawJsonb::new(data);
        from_raw_jsonb(&raw_jsonb).expect("Failed to deserialize Number from jsonb bytes")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        jsonb::Value::Number(value.0.clone()).to_vec()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Number")
    }
}

impl redb::Value for RawObject {
    type SelfType<'a>
        = RawObject
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        RawObject(data.to_vec())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.as_slice()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("RawObject")
    }
}

impl redb::Key for Number {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let num1 = <Number as redb::Value>::from_bytes(data1);
        let num2 = <Number as redb::Value>::from_bytes(data2);
        num1.0.partial_cmp(&num2.0).unwrap()
    }
}

impl redb::Value for Int {
    type SelfType<'a>
        = i64
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        i64::from_be_bytes(data.try_into().expect("Invalid 8-byte slice"))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_be_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Int")
    }
}

impl redb::Key for Int {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        i64::from_be_bytes(data1.try_into().expect("Invalid 8-byte slice")).cmp(
            &i64::from_be_bytes(data2.try_into().expect("Invalid 8-byte slice")),
        )
    }
}
