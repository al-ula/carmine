use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use serde::Deserialize;

use crate::api::error::ApiError;
use crate::AppState;
use carmine_core::{
    meta::{CabinetMeta, ShelfMeta},
    cabinet::Cabinet,
    transaction::Writable,
};

#[derive(Deserialize)]
pub struct CreateCabinetRequest {
    name: String,
}

#[derive(Deserialize)]
pub struct CreateShelfRequest {
    name: String,
    key_type: String,
    value_type: String,
}

pub async fn create_cabinet(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateCabinetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if state.system_store.find_cabinet_by_name(&req.name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .is_some() 
    {
        return Err(ApiError::CabinetAlreadyExists(req.name));
    }

    let id: u64 = small_uid::SmallUid::new().into();
    let path = state.data_dir.join(format!("cabinet_{}", id));

    let cabinet = Cabinet::create(
        id,
        req.name.clone(),
        path.clone(),
        state.cabinet_cache_size,
    )
    .map_err(|e| ApiError::Internal(e.to_string()))?;

    let meta = CabinetMeta {
        id,
        name: req.name,
        path,
        shelves: Vec::new(),
    };

    state.system_store.register_cabinet(&meta)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    state.cabinets.insert(id, cabinet);

    Ok(Json(meta))
}

pub async fn list_cabinets(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    let cabinets = state.system_store.list_cabinets()
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(cabinets))
}

pub async fn get_cabinet(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let meta = state.system_store.find_cabinet_by_name(&name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(name))?;
    Ok(Json(meta))
}

pub async fn delete_cabinet(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let meta = state.system_store.find_cabinet_by_name(&name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(name.clone()))?;

    state.cabinets.remove(&meta.id);

    std::fs::remove_file(&meta.path)
        .map_err(|e| ApiError::Internal(format!("Failed to remove cabinet file: {}", e)))?;

    state.system_store.remove_cabinet(meta.id)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn clean_cabinet(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let meta = state.system_store.find_cabinet_by_name(&name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(name))?;

    let cabinet = state.get_or_open_cabinet(meta.id, meta.name.clone(), meta.path.clone())
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    let db = cabinet.database();

    let txn = db.begin_write()
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    for shelf_meta in &meta.shelves {
        let key_type = match shelf_meta.key_type.as_str() {
            "String" => carmine_core::key::KeyType::String,
            "Number" => carmine_core::key::KeyType::Number,
            "Int" => carmine_core::key::KeyType::Int,
            _ => continue,
        };
        let value_type = match shelf_meta.value_type.as_str() {
            "String" => carmine_core::value::ValueType::String,
            "Number" => carmine_core::value::ValueType::Number,
            "Int" => carmine_core::value::ValueType::Int,
            "Object" => carmine_core::value::ValueType::Object,
            "Byte" => carmine_core::value::ValueType::Byte,
            _ => continue,
        };
        let shelf = carmine_core::shelf::Shelf::new(
            shelf_meta.name.clone(),
            key_type,
            value_type,
        );
        let _ = shelf.clear(&txn);
    }

    txn.commit()
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(axum::http::StatusCode::NO_CONTENT)
}

pub async fn create_shelf(
    State(state): State<Arc<AppState>>,
    Path(cabinet_name): Path<String>,
    Json(req): Json<CreateShelfRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut meta = state.system_store.find_cabinet_by_name(&cabinet_name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(cabinet_name.clone()))?;

    if meta.shelves.iter().any(|s| s.name == req.name) {
        return Err(ApiError::ShelfAlreadyExists(req.name));
    }

    let shelf_meta = ShelfMeta {
        name: req.name.clone(),
        key_type: req.key_type,
        value_type: req.value_type,
    };

    meta.shelves.push(shelf_meta.clone());
    state.system_store.update_cabinet(&meta)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(shelf_meta))
}

pub async fn list_shelves(
    State(state): State<Arc<AppState>>,
    Path(cabinet_name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let meta = state.system_store.find_cabinet_by_name(&cabinet_name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(cabinet_name))?;
    Ok(Json(meta.shelves))
}

pub async fn delete_shelf(
    State(state): State<Arc<AppState>>,
    Path((cabinet_name, shelf_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let mut meta = state.system_store.find_cabinet_by_name(&cabinet_name)
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::CabinetNotFound(cabinet_name.clone()))?;

    if !meta.shelves.iter().any(|s| s.name == shelf_name) {
        return Err(ApiError::ShelfNotFound(shelf_name));
    }

    meta.shelves.retain(|s| s.name != shelf_name);
    state.system_store.update_cabinet(&meta)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(axum::http::StatusCode::NO_CONTENT)
}
