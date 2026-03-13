use thiserror::Error;

use crate::{bucket::Bucket, tub::Tub};

mod get;
pub use get::*;

mod put;
pub use put::*;

mod set;
pub use set::*;

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("Failed to begin read/write transaction: {0}")]
    RedbTransactionError(#[from] redb::TransactionError),
    #[error("Storage error: {0}")]
    StorageError(#[from] redb::StorageError),
    #[error("Commit error: {0}")]
    CommitError(#[from] redb::CommitError),
    #[error("Key already exists")]
    KeyAlreadyExists,
    #[error("Key type mismatch: expected {expected:?}, got {actual:?}")]
    KeyTypeMismatch {
        expected: crate::key::KeyType,
        actual: crate::key::KeyType,
    },
    #[error("Value type mismatch: expected {expected:?}, got {actual:?}")]
    ValueTypeMismatch {
        expected: crate::value::ValueType,
        actual: crate::value::ValueType,
    },
}

pub struct Transaction<'a> {
    tub: &'a Tub,
    bucket: &'a Bucket,
}

impl<'a> Transaction<'a> {
    pub fn new(tub: &'a Tub, bucket: &'a Bucket) -> Self {
        Self { tub, bucket }
    }

    pub fn validate_key_type(&self, key: &crate::key::Key) -> Result<(), TransactionError> {
        let actual = key.as_type();
        if actual != self.bucket.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.bucket.key_type,
                actual,
            });
        }
        Ok(())
    }

    pub fn validate_value_type(&self, value: &crate::value::Value) -> Result<(), TransactionError> {
        let actual = value.as_type();
        if actual != self.bucket.value_type {
            return Err(TransactionError::ValueTypeMismatch {
                expected: self.bucket.value_type,
                actual,
            });
        }
        Ok(())
    }
}
