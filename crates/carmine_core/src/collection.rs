use redb::{Key, ReadableDatabase, TableDefinition, Value};

use crate::{
    Result,
    error::Error,
    key::{KeyType, KeyTypes},
    store::Store,
    value::{ValueType, ValueTypes},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Collection {
    name: String,
    key_type: KeyTypes,
    value_type: ValueTypes,
}

#[derive(Debug)]
pub struct CollectionBuilder {
    name: String,
    key_type: KeyTypes,
    value_type: ValueTypes,
}

impl Collection {
    pub fn builder(name: &str) -> CollectionBuilder {
        CollectionBuilder {
            name: name.to_string(),
            key_type: KeyTypes::String,
            value_type: ValueTypes::Bytes,
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn key_type(&self) -> KeyTypes {
        self.key_type
    }
    pub fn value_type(&self) -> ValueTypes {
        self.value_type
    }
}

impl CollectionBuilder {
    pub fn key_type(mut self, key_type: KeyTypes) -> Self {
        self.key_type = key_type;
        self
    }

    pub fn value_type(mut self, value_type: ValueTypes) -> Self {
        self.value_type = value_type;
        self
    }

    pub fn build(self) -> Collection {
        Collection {
            name: self.name,
            key_type: self.key_type,
            value_type: self.value_type,
        }
    }

    pub fn get(&self, key: impl KeyType, store: Store) -> Result<Box<dyn ValueType>> {
        match (self.key_type, self.value_type) {
            (KeyTypes::String, ValueTypes::Bytes) => {
                let key = key.as_str()?.to_owned();
                let result = get::<String, Vec<u8>>(key, &self.name, store)?;
                Ok(Box::new(result))
            }
            (KeyTypes::String, ValueTypes::String) => {
                let key = key.as_str()?.to_owned();
                let result = get::<String, String>(key, &self.name, store)?;
                Ok(Box::new(result))
            }
            // Add more type combinations as needed
            _ => Err(Error::Validation(
                "Unsupported key/value type combination".to_string(),
            )),
        }
    }
}

fn get<K, V>(key: K, name: &str, db: Store) -> Result<V>
where
    K: Key + for<'a> std::borrow::Borrow<<K as redb::Value>::SelfType<'a>> + 'static,
    V: Value + for<'a> std::convert::From<<V as redb::Value>::SelfType<'a>> + 'static,
{
    let table_def: TableDefinition<K, V> = TableDefinition::new(name);
    let db = db.handle;
    let read_tx = db.begin_read()?;
    let table = read_tx.open_table(table_def)?;
    if let Some(value) = table.get(key)? {
        Ok(V::from(value.value()))
    } else {
        Err(Error::Validation(format!(
            "Key not found in collection {}",
            name
        )))
    }
}
