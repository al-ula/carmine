mod number;
use crate::value::{ValueType, ValueTypes};
pub use number::Number;

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

    fn as_bytes(&self) -> Option<&[u8]> {
        Some(&self.0)
    }
}
