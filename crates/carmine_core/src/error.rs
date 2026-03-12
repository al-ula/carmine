use thiserror::Error;

use crate::key::KeyError;
use crate::transaction::TransactionError;
use crate::types::TypesError;
use crate::value::ValueError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Key error: {0}")]
    Key(#[from] KeyError),
    #[error("Value error: {0}")]
    Value(#[from] ValueError),
    #[error("Types error: {0}")]
    Types(#[from] TypesError),
    #[error("Bucket error: {0}")]
    Bucket(#[from] crate::bucket::BucketError),
    #[error("Tub error: {0}")]
    Tub(#[from] crate::tub::TubError),
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),
}
