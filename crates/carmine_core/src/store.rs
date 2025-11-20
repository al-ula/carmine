use redb::StorageBackend;

use crate::{
    validation::{validate_collection_name, validate_store_name},
    Result,
};
use std::{fs::File, path::Path};
use crate::collection::Collection;
use crate::key::KeyTypes;
use crate::value::ValueTypes;

#[derive(Debug)]
pub struct Store {
    pub name: String,
    pub handle: redb::Database,
}

impl Store {
    pub fn create(name: String, path: impl AsRef<Path>) -> Result<Self> {
        validate_store_name(&name)?;
        let db = redb::Database::create(path)?;
        Ok(Self { name, handle: db })
    }
    pub fn rename(&mut self, name: String) -> Result<()> {
        validate_store_name(&name)?;
        self.name = name;
        Ok(())
    }
    pub fn open(name: String, path: impl AsRef<Path>) -> Result<Self> {
        validate_store_name(&name)?;
        let db = redb::Database::open(path)?;
        Ok(Self { name, handle: db })
    }
    pub fn compact(&mut self) -> Result<()> {
        self.handle.compact()?;
        Ok(())
    }
    pub fn check_integrity(&mut self) -> Result<()> {
        self.handle.check_integrity()?;
        Ok(())
    }
    pub fn begin_write(&self) -> Result<redb::WriteTransaction> {
        self.handle.begin_write().map_err(|e| e.into())
    }
    pub fn builder(name: impl Into<String>) -> Result<Builder> {
        Builder::new(name)
    }
    pub fn collection(
        &self,
        name: impl Into<String>,
        key_type: KeyTypes,
        value_type: ValueTypes,
    ) -> Result<Collection> {
        let name = name.into();
        validate_collection_name(&name)?;
        Ok(Collection {
            name,
            key_type,
            value_type,
        })
    }
}

impl redb::ReadableDatabase for Store {
    fn begin_read(&self) -> std::result::Result<redb::ReadTransaction, redb::TransactionError> {
        redb::ReadableDatabase::begin_read(&self.handle)
    }

    fn cache_stats(&self) -> redb::CacheStats {
        self.handle.cache_stats()
    }
}

pub struct ReadOnlyStore {
    pub name: String,
    pub handle: redb::ReadOnlyDatabase,
}

impl ReadOnlyStore {
    pub fn open(name: String, path: impl AsRef<Path>) -> Result<Self> {
        validate_store_name(&name)?;
        let db = redb::ReadOnlyDatabase::open(path)?;
        Ok(Self { name, handle: db })
    }
    pub fn collection(
        &self,
        name: impl Into<String>,
        key_type: KeyTypes,
        value_type: ValueTypes,
    ) -> Result<Collection> {
        let name = name.into();
        validate_collection_name(&name)?;
        Ok(Collection {
            name,
            key_type,
            value_type,
        })
    }
}

impl redb::ReadableDatabase for ReadOnlyStore {
    fn begin_read(&self) -> std::result::Result<redb::ReadTransaction, redb::TransactionError> {
        self.handle.begin_read()
    }

    fn cache_stats(&self) -> redb::CacheStats {
        self.handle.cache_stats()
    }
}

pub struct Builder {
    name: String,
    redb: redb::Builder,
}

impl Builder {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = validate_store_name(&name.into())?;
        Ok(Self {
            name,
            redb: redb::Builder::new(),
        })
    }
    pub fn set_repair_callback(
        &mut self,
        callback: impl Fn(&mut redb::RepairSession) + 'static,
    ) -> &mut Self {
        self.redb.set_repair_callback(callback);
        self
    }
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.redb.set_cache_size(bytes);
        self
    }
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Store> {
        validate_store_name(&self.name)?;
        let db = self.redb.create(path)?;
        Ok(Store {
            name: self.name.clone(),
            handle: db,
        })
    }
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Store> {
        validate_store_name(&self.name)?;
        let redb_db = self.redb.open(path)?;
        Ok(Store {
            name: self.name.clone(),
            handle: redb_db,
        })
    }
    pub fn open_read_only(&self, path: impl AsRef<Path>) -> Result<ReadOnlyStore> {
        validate_store_name(&self.name)?;
        let redb_db = self.redb.open_read_only(path)?;
        Ok(ReadOnlyStore {
            name: self.name.clone(),
            handle: redb_db,
        })
    }
    pub fn create_file(&self, file: File) -> Result<Store> {
        validate_store_name(&self.name)?;
        let db = self.redb.create_file(file)?;
        Ok(Store {
            name: self.name.clone(),
            handle: db,
        })
    }
    pub fn create_with_backend(&self, backend: impl StorageBackend) -> Result<Store> {
        validate_store_name(&self.name)?;
        let db = self.redb.create_with_backend(backend)?;
        Ok(Store {
            name: self.name.clone(),
            handle: db,
        })
    }
}
