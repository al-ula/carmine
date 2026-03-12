use thiserror::Error;

use crate::{key::KeyType, value::ValueType};

#[derive(Debug, Clone)]
pub struct Bucket {
    pub name: String,
    pub key_type: KeyType,
    pub value_type: ValueType,
}

#[derive(Debug, Error)]
pub enum BucketError {
    #[error("Redb table error: {0}")]
    RedbTableError(#[from] redb::TableError),
}

impl Bucket {
    pub fn new(name: String, key_type: KeyType, value_type: ValueType) -> Self {
        Self {
            name,
            key_type,
            value_type,
        }
    }
}
