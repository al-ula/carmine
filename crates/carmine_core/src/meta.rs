use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::cabinet::Cabinet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShelfMeta {
    pub name: String,
    pub key_type: String,
    pub value_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CabinetMeta {
    pub id: u64,
    pub name: String,
    pub path: PathBuf,
    #[serde(default)]
    pub shelves: Vec<ShelfMeta>,
}

impl From<&Cabinet> for CabinetMeta {
    fn from(cabinet: &Cabinet) -> Self {
        Self {
            id: cabinet.id,
            name: cabinet.name.clone(),
            path: cabinet.path.clone(),
            shelves: Vec::new(),
        }
    }
}

impl CabinetMeta {
    pub fn open(self, cache_size: usize) -> Result<Cabinet, crate::cabinet::CabinetError> {
        Cabinet::open(self.id, self.name, self.path, cache_size)
    }
}
