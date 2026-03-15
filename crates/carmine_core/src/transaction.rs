use redb::ReadableDatabase;
use thiserror::Error;

use crate::{
    cabinet::Cabinet,
    key::Key,
    shelf::Shelf,
    value::{Value, ValueRetVec},
};

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
    #[error("Range queries are not supported for Number keys")]
    RangeNotSupported,
}

impl From<crate::error::Error> for TransactionError {
    fn from(e: crate::error::Error) -> Self {
        TransactionError::StorageError(redb::StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))
    }
}

impl From<redb::TableError> for TransactionError {
    fn from(e: redb::TableError) -> Self {
        TransactionError::StorageError(redb::StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))
    }
}

pub struct TransactionOld<'a> {
    cabinet: &'a Cabinet,
    shelf: &'a Shelf,
}

impl<'a> TransactionOld<'a> {
    pub fn new(cabinet: &'a Cabinet, shelf: &'a Shelf) -> Self {
        Self { cabinet, shelf }
    }

    pub fn validate_key_type(&self, key: &crate::key::Key) -> Result<(), TransactionError> {
        let actual = key.as_type();
        if actual != self.shelf.key_type {
            return Err(TransactionError::KeyTypeMismatch {
                expected: self.shelf.key_type,
                actual,
            });
        }
        Ok(())
    }

    pub fn validate_value_type(&self, value: &crate::value::Value) -> Result<(), TransactionError> {
        let actual = value.as_type();
        if actual != self.shelf.value_type {
            return Err(TransactionError::ValueTypeMismatch {
                expected: self.shelf.value_type,
                actual,
            });
        }
        Ok(())
    }
}

#[inline(always)]
pub fn begin_read(cabinet: &Cabinet) -> Result<redb::ReadTransaction, crate::error::Error> {
    cabinet
        .database()
        .begin_read()
        .map_err(|e| crate::error::Error::Transaction(e.into()))
}

#[inline(always)]
pub fn begin_write(cabinet: &Cabinet) -> Result<redb::WriteTransaction, crate::error::Error> {
    cabinet
        .database()
        .begin_write()
        .map_err(|e| crate::error::Error::Transaction(e.into()))
}

#[inline(always)]
pub fn commit_transaction(tx: redb::WriteTransaction) -> Result<(), TransactionError> {
    tx.commit()?;
    Ok(())
}

pub trait Readable {
    fn get(&self, tx: &redb::ReadTransaction, key: &Key)
        -> Result<Option<Value>, TransactionError>;
    fn get_all(&self, tx: &redb::ReadTransaction) -> Result<Vec<(Key, Value)>, TransactionError>;
    fn keys(&self, tx: &redb::ReadTransaction) -> Result<Vec<Key>, TransactionError>;
    fn values(&self, tx: &redb::ReadTransaction) -> Result<Vec<Value>, TransactionError>;
    fn get_range(
        &self,
        tx: &redb::ReadTransaction,
        start: &Key,
        end: &Key,
    ) -> Result<Vec<(Key, Value)>, TransactionError>;
    fn get_batch(
        &self,
        tx: &redb::ReadTransaction,
        keys: &[Key],
    ) -> Result<ValueRetVec, TransactionError>;
    fn exists(&self, tx: &redb::ReadTransaction, key: &Key) -> Result<bool, TransactionError>;
    fn count(&self, tx: &redb::ReadTransaction) -> Result<u64, TransactionError>;
}

pub trait Writable: Readable {
    fn set(
        &self,
        tx: &redb::WriteTransaction,
        key: Key,
        value: Value,
    ) -> Result<(), TransactionError>;
    fn put(
        &self,
        tx: &redb::WriteTransaction,
        key: Key,
        value: Value,
    ) -> Result<(), TransactionError>;
    fn delete(&self, tx: &redb::WriteTransaction, key: &Key) -> Result<bool, TransactionError>;
    fn batch_set(
        &self,
        tx: &redb::WriteTransaction,
        entries: &[(Key, Value)],
    ) -> Result<Vec<Result<(), TransactionError>>, TransactionError>;
    fn batch_put(
        &self,
        tx: &redb::WriteTransaction,
        entries: &[(Key, Value)],
    ) -> Result<Vec<Result<(), TransactionError>>, TransactionError>;
    fn batch_delete(
        &self,
        tx: &redb::WriteTransaction,
        keys: &[Key],
    ) -> Result<Vec<bool>, TransactionError>;
    fn clear(&self, tx: &redb::WriteTransaction) -> Result<u64, TransactionError>;
}
