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
}

pub struct Transaction<'a> {
    tub: &'a Tub,
    bucket: &'a Bucket,
}

impl<'a> Transaction<'a> {
    pub fn new(tub: &'a Tub, bucket: &'a Bucket) -> Self {
        Self { tub, bucket }
    }
}
