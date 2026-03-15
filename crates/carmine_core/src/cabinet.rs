use redb::{Builder, Database, DatabaseError};
use std::sync::Arc;
use std::{io, path::PathBuf};
use thiserror::Error;

/// Default page cache size for cabinet databases (64 MB).
pub const DEFAULT_CACHE_SIZE: usize = 64 * 1024 * 1024;

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
    pub id: u64,
    pub name: String,
    pub path: PathBuf,
    db: Arc<Database>,
}

impl Cabinet {
    pub fn create(id: u64, name: String, path: PathBuf, cache_size: usize) -> Result<Self, Error> {
        let db = Builder::new().set_cache_size(cache_size).create(&path)?;
        Ok(Self {
            id,
            name,
            path,
            db: Arc::new(db),
        })
    }

    pub fn open(id: u64, name: String, path: PathBuf, cache_size: usize) -> Result<Self, Error> {
        let db = Builder::new().set_cache_size(cache_size).open(&path)?;
        Ok(Self {
            id,
            name,
            path,
            db: Arc::new(db),
        })
    }

    pub fn database(&self) -> &Database {
        &self.db
    }
}
