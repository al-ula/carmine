use axum::extract::{Path, State};
use std::sync::Arc;

use crate::api::error::ApiError;
use crate::AppState;
use carmine_core::shelf::Shelf;

#[derive(Debug, Clone)]
pub struct ResolvedShelf {
    pub cabinet: carmine_core::cabinet::Cabinet,
    pub shelf: Shelf,
}

pub async fn resolve_shelf(
    State(state): State<Arc<AppState>>,
    Path((cabinet_name, shelf_name)): Path<(String, String)>,
) -> Result<ResolvedShelf, ApiError> {
    let cabinet_meta = state
        .system_store
        .find_cabinet_by_name(&cabinet_name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(cabinet_name.clone()))?;

    let shelf_meta = cabinet_meta
        .shelves
        .iter()
        .find(|s| s.name == shelf_name)
        .ok_or_else(|| ApiError::ShelfNotFound(shelf_name.clone()))?;

    let key_type = match shelf_meta.key_type.as_str() {
        "String" => carmine_core::key::KeyType::String,
        "Number" => carmine_core::key::KeyType::Number,
        "Int" => carmine_core::key::KeyType::Int,
        _ => return Err(ApiError::Internal(format!("Unknown key type: {}", shelf_meta.key_type))),
    };

    let value_type = match shelf_meta.value_type.as_str() {
        "String" => carmine_core::value::ValueType::String,
        "Number" => carmine_core::value::ValueType::Number,
        "Int" => carmine_core::value::ValueType::Int,
        "Object" => carmine_core::value::ValueType::Object,
        "Byte" => carmine_core::value::ValueType::Byte,
        _ => return Err(ApiError::Internal(format!("Unknown value type: {}", shelf_meta.value_type))),
    };

    let shelf = Shelf::new(shelf_name.clone(), key_type, value_type);

    let cabinet = state
        .get_or_open_cabinet(cabinet_meta.id, cabinet_meta.name, cabinet_meta.path)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(ResolvedShelf { cabinet, shelf })
}
