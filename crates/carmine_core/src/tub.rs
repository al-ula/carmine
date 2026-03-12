use redb::{Database, DatabaseError};
use std::{io, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TubError {
    #[error("Failed to register tub: {0}")]
    Register(#[from] io::Error),
    #[error("Failed to open tub: {0}")]
    Database(#[from] DatabaseError),
}
type Error = TubError;
#[derive(Debug, Clone)]
pub struct Tub {
    pub id: String,
    pub name: String,
}

impl Tub {
    pub fn new(id: String, name: String) -> Self {
        Self { id, name }
    }
    pub fn create(name: String) -> Result<Self, Error> {
        let (id, path) = register_tub(name.clone())?;
        let tub = Self { id, name };
        let _database = Database::create(path)?;
        Ok(tub)
    }

    pub fn open(&self) -> Result<Database, DatabaseError> {
        let path = get_tub_path(self);
        Database::open(&path)
    }
}

fn register_tub(name: String) -> Result<(String, PathBuf), io::Error> {
    todo!()
}
fn get_tub_path(tub: &Tub) -> PathBuf {
    todo!()
}
