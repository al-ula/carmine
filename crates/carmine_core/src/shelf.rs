use thiserror::Error;

use crate::{key::KeyType, value::ValueType};

#[derive(Debug, Clone)]
pub struct Shelf {
    pub name: String,
    pub key_type: KeyType,
    pub value_type: ValueType,
}

#[derive(Debug, Error)]
pub enum ShelfError {
    #[error("Redb table error: {0}")]
    RedbTableError(#[from] redb::TableError),
}

impl Shelf {
    pub fn new(name: String, key_type: KeyType, value_type: ValueType) -> Self {
        Self {
            name,
            key_type,
            value_type,
        }
    }
}
