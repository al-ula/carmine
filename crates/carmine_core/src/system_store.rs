use std::path::Path;

use jsonb::RawJsonb;
use redb::{Builder, Database, ReadableDatabase, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::meta::{CabinetMeta, ShelfMeta};

const CABINETS: TableDefinition<u64, &[u8]> = TableDefinition::new("cabinets");

pub const DEFAULT_CACHE_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum SystemStoreError {
    #[error("Database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("Storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("Table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("Transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("Commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("Serialization error: {0}")]
    Jsonb(String),
}

pub struct SystemStore {
    db: Database,
}

impl SystemStore {
    pub fn open(path: &Path, cache_size: usize) -> Result<Self, SystemStoreError> {
        let db = Builder::new().set_cache_size(cache_size).create(path)?;
        Ok(Self { db })
    }

    pub fn register_cabinet(&self, meta: &CabinetMeta) -> Result<(), SystemStoreError> {
        let owned =
            jsonb::to_owned_jsonb(&meta).map_err(|e| SystemStoreError::Jsonb(e.to_string()))?;
        let bytes: &[u8] = owned.as_ref();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(CABINETS)?;
            table.insert(meta.id, bytes)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get_cabinet(&self, id: u64) -> Result<Option<CabinetMeta>, SystemStoreError> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(CABINETS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let value = table.get(id)?;
        match value {
            Some(raw) => {
                let raw_jsonb = RawJsonb::new(raw.value());
                let meta: CabinetMeta = jsonb::from_raw_jsonb(&raw_jsonb)
                    .map_err(|e| SystemStoreError::Jsonb(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub fn list_cabinets(&self) -> Result<Vec<CabinetMeta>, SystemStoreError> {
        let txn = self.db.begin_read()?;
        let table = match txn.open_table(CABINETS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut cabinets = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry?;
            let raw_jsonb = RawJsonb::new(value.value());
            let meta: CabinetMeta = jsonb::from_raw_jsonb(&raw_jsonb)
                .map_err(|e| SystemStoreError::Jsonb(e.to_string()))?;
            cabinets.push(meta);
        }
        Ok(cabinets)
    }

    pub fn find_cabinet_by_name(
        &self,
        name: &str,
    ) -> Result<Option<CabinetMeta>, SystemStoreError> {
        let cabinets = self.list_cabinets()?;
        Ok(cabinets.into_iter().find(|c| c.name == name))
    }

    pub fn update_cabinet(&self, meta: &CabinetMeta) -> Result<(), SystemStoreError> {
        self.register_cabinet(meta)
    }

    pub fn remove_cabinet(&self, id: u64) -> Result<bool, SystemStoreError> {
        let txn = self.db.begin_write()?;
        let removed = {
            let mut table = txn.open_table(CABINETS)?;
            table.remove(id)?.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }

    pub fn add_shelf(&self, cabinet_id: u64, shelf: ShelfMeta) -> Result<(), SystemStoreError> {
        let mut cabinet = self
            .get_cabinet(cabinet_id)?
            .ok_or_else(|| SystemStoreError::Jsonb("Cabinet not found".to_string()))?;

        if cabinet.shelves.iter().any(|s| s.name == shelf.name) {
            return Err(SystemStoreError::Jsonb("Shelf already exists".to_string()));
        }

        cabinet.shelves.push(shelf);
        self.update_cabinet(&cabinet)
    }

    pub fn remove_shelf(
        &self,
        cabinet_id: u64,
        shelf_name: &str,
    ) -> Result<bool, SystemStoreError> {
        let mut cabinet = self
            .get_cabinet(cabinet_id)?
            .ok_or_else(|| SystemStoreError::Jsonb("Cabinet not found".to_string()))?;

        let initial_len = cabinet.shelves.len();
        cabinet.shelves.retain(|s| s.name != shelf_name);

        if cabinet.shelves.len() == initial_len {
            return Ok(false);
        }

        self.update_cabinet(&cabinet)?;
        Ok(true)
    }
}
