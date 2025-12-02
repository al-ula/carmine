use std::{panic::Location, sync::PoisonError};

use thiserror::Error;

use crate::{key::KeyTypes, value::ValueTypes};

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Redb(#[from] redb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Poisoned internal lock: {0}")]
    LockPoisoned(&'static Location<'static>),
    #[error("Key type mismatch.\nExpected {e:?} got {g:?}")]
    KeyTypeMismatch { e: KeyTypes, g: KeyTypes },
    #[error("Value type mismatch.\nExpected {e:?} got {g:?}")]
    ValueTypeMismatch { e: ValueTypes, g: ValueTypes },
}

impl From<redb::CommitError> for Error {
    fn from(value: redb::CommitError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::TransactionError> for Error {
    fn from(value: redb::TransactionError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::CompactionError> for Error {
    fn from(value: redb::CompactionError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(value: redb::DatabaseError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::SavepointError> for Error {
    fn from(value: redb::SavepointError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::SetDurabilityError> for Error {
    fn from(value: redb::SetDurabilityError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::StorageError> for Error {
    fn from(value: redb::StorageError) -> Self {
        Error::Redb(value.into())
    }
}

impl From<redb::TableError> for Error {
    fn from(value: redb::TableError) -> Self {
        Error::Redb(value.into())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Error {
        Error::LockPoisoned(Location::caller())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redb_error_conversion() {
        let redb_error = redb::Error::DatabaseAlreadyOpen;
        let error: Error = redb_error.into();
        match error {
            Error::Redb(e) => assert!(matches!(e, redb::Error::DatabaseAlreadyOpen)),
            _ => panic!("Unexpected error"),
        }
    }
}
