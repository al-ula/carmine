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
    #[error("Shelf error: {0}")]
    Shelf(#[from] crate::shelf::ShelfError),
    #[error("Cabinet error: {0}")]
    Cabinet(#[from] crate::cabinet::CabinetError),
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),
}
