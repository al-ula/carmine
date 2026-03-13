use redb::{Database, DatabaseError};
use std::{io, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CabinetError {
    #[error("Failed to register cabinet: {0}")]
    Register(#[from] io::Error),
    #[error("Failed to open cabinet: {0}")]
    Database(#[from] DatabaseError),
}
type Error = CabinetError;
#[derive(Debug, Clone)]
pub struct Cabinet {
    pub id: String,
    pub name: String,
}

impl Cabinet {
    pub fn new(id: String, name: String) -> Self {
        Self { id, name }
    }
    pub fn create(name: String) -> Result<Self, Error> {
        let (id, path) = register_cabinet(name.clone())?;
        let cabinet = Self { id, name };
        let _database = Database::create(path)?;
        Ok(cabinet)
    }

    pub fn open(&self) -> Result<Database, DatabaseError> {
        let path = get_cabinet_path(self);
        Database::open(&path)
    }
}

fn register_cabinet(name: String) -> Result<(String, PathBuf), io::Error> {
    todo!()
}
fn get_cabinet_path(cabinet: &Cabinet) -> PathBuf {
    todo!()
}
