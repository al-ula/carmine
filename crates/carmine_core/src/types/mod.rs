mod number;
pub use number::Number;
use crate::value::{ValueType, ValueTypes};

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