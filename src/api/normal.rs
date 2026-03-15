use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use redb::ReadableDatabase;

use axum::extract::Path;

use crate::api::error::ApiError;
use crate::api::extractors::resolve_shelf;
use crate::AppState;
use carmine_core::{
    key::Key,
    transaction::{Readable, Writable},
    types::{Int, Number, RawObject},
    value::Value,
};

// --- Parsing helpers: RawJsonb → Key/Value ---

fn parse_body(body: &Bytes) -> Result<jsonb::OwnedJsonb, ApiError> {
    jsonb::parse_owned_jsonb(body.as_ref()).map_err(|e| ApiError::JsonParse(e.to_string()))
}

fn get_field(raw: &jsonb::RawJsonb, name: &str) -> Result<jsonb::OwnedJsonb, ApiError> {
    raw.get_by_name(name, false)
        .map_err(|e| ApiError::JsonParse(e.to_string()))?
        .ok_or_else(|| ApiError::JsonParse(format!("missing field '{}'", name)))
}

fn owned_to_key(owned: &jsonb::OwnedJsonb) -> Result<Key, ApiError> {
    let raw = owned.as_raw();
    // Try string first
    if let Ok(s) = jsonb::from_raw_jsonb::<String>(&raw) {
        return Ok(Key::String(s));
    }
    // Try i64
    if let Ok(i) = jsonb::from_raw_jsonb::<i64>(&raw) {
        return Ok(Key::Int(Int::from(i)));
    }
    // Try number
    if let Ok(n) = jsonb::from_raw_jsonb::<jsonb::Number>(&raw) {
        return Ok(Key::Number(Number::from(n)));
    }
    Err(ApiError::JsonParse("key must be a string or number".into()))
}

fn owned_to_value(owned: &jsonb::OwnedJsonb) -> Result<Value, ApiError> {
    let raw = owned.as_raw();
    // Try string
    if let Ok(s) = jsonb::from_raw_jsonb::<String>(&raw) {
        return Ok(Value::String(s));
    }
    // Try i64
    if let Ok(i) = jsonb::from_raw_jsonb::<i64>(&raw) {
        return Ok(Value::Int(Int::from(i)));
    }
    // Try number
    if let Ok(n) = jsonb::from_raw_jsonb::<jsonb::Number>(&raw) {
        return Ok(Value::Number(Number::from(n)));
    }
    // Object: the raw jsonb bytes ARE the RawObject
    // Check if it's a valid object by trying to get keys
    if raw.object_keys().ok().flatten().is_some() {
        return Ok(Value::Object(RawObject::from(owned.clone().to_vec())));
    }
    Err(ApiError::JsonParse("value must be a string, number, or object".into()))
}

// --- Serialization helpers: Key/Value → OwnedJsonb ---

fn key_to_owned(key: &Key) -> Result<jsonb::OwnedJsonb, ApiError> {
    match key {
        Key::String(s) => jsonb::to_owned_jsonb(s),
        Key::Int(i) => jsonb::to_owned_jsonb(&**i),
        Key::Number(n) => jsonb::to_owned_jsonb(&**n),
    }
    .map_err(|e| ApiError::Internal(e.to_string()))
}

fn value_to_owned(value: &Value) -> Result<jsonb::OwnedJsonb, ApiError> {
    match value {
        Value::String(s) => jsonb::to_owned_jsonb(s)
            .map_err(|e| ApiError::Internal(e.to_string())),
        Value::Int(i) => jsonb::to_owned_jsonb(&**i)
            .map_err(|e| ApiError::Internal(e.to_string())),
        Value::Number(n) => jsonb::to_owned_jsonb(&**n)
            .map_err(|e| ApiError::Internal(e.to_string())),
        Value::Object(o) => {
            // RawObject is already jsonb bytes
            Ok(jsonb::OwnedJsonb::new(o.to_vec()))
        }
        Value::Byte(b) => {
            use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
            jsonb::to_owned_jsonb(&BASE64.encode(b))
                .map_err(|e| ApiError::Internal(e.to_string()))
        }
    }
}

fn build_response(fields: &[(&str, jsonb::OwnedJsonb)]) -> Result<Response, ApiError> {
    let items: Vec<_> = fields.iter().map(|(k, v)| (*k, v.as_raw())).collect();
    let obj = jsonb::OwnedJsonb::build_object(items)
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    let json_text = obj.as_raw().to_string();
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        json_text,
    ).into_response())
}

// --- Handlers ---

pub async fn set(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let raw = parsed.as_raw();
    let key = owned_to_key(&get_field(&raw, "key")?)?;
    let value = owned_to_value(&get_field(&raw, "value")?)?;

    if key.as_type() != resolved.shelf.key_type {
        return Err(ApiError::KeyTypeMismatch {
            expected: resolved.shelf.key_type,
            actual: key.as_type(),
        });
    }
    if value.as_type() != resolved.shelf.value_type {
        return Err(ApiError::ValueTypeMismatch {
            expected: resolved.shelf.value_type,
            actual: value.as_type(),
        });
    }

    let db = resolved.cabinet.database();
    let tx = db.begin_write().map_err(|e| ApiError::Internal(e.to_string()))?;
    resolved.shelf.set(&tx, key, value).map_err(|e| ApiError::Internal(e.to_string()))?;
    tx.commit().map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn put(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let raw = parsed.as_raw();
    let key = owned_to_key(&get_field(&raw, "key")?)?;
    let value = owned_to_value(&get_field(&raw, "value")?)?;

    if key.as_type() != resolved.shelf.key_type {
        return Err(ApiError::KeyTypeMismatch {
            expected: resolved.shelf.key_type,
            actual: key.as_type(),
        });
    }
    if value.as_type() != resolved.shelf.value_type {
        return Err(ApiError::ValueTypeMismatch {
            expected: resolved.shelf.value_type,
            actual: value.as_type(),
        });
    }

    let db = resolved.cabinet.database();
    let tx = db.begin_write().map_err(|e| ApiError::Internal(e.to_string()))?;
    resolved.shelf.put(&tx, key, value).map_err(|e| ApiError::Internal(e.to_string()))?;
    tx.commit().map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let raw = parsed.as_raw();
    let key = owned_to_key(&get_field(&raw, "key")?)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let value = resolved.shelf.get(&tx, &key).map_err(|e| ApiError::Internal(e.to_string()))?;

    let val_jsonb = match value {
        Some(val) => value_to_owned(&val)?,
        None => jsonb::to_owned_jsonb(&()).map_err(|e| ApiError::Internal(e.to_string()))?,
    };
    build_response(&[("value", val_jsonb)])
}

pub async fn delete(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let raw = parsed.as_raw();
    let key = owned_to_key(&get_field(&raw, "key")?)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_write().map_err(|e| ApiError::Internal(e.to_string()))?;
    resolved.shelf.delete(&tx, &key).map_err(|e| ApiError::Internal(e.to_string()))?;
    tx.commit().map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn all(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let entries = resolved.shelf.get_all(&tx).map_err(|e| ApiError::Internal(e.to_string()))?;

    let entry_jsonbs: Result<Vec<_>, _> = entries.iter().map(|(k, v)| {
        let ko = key_to_owned(k)?;
        let vo = value_to_owned(v)?;
        jsonb::OwnedJsonb::build_array([ko.as_raw(), vo.as_raw()])
            .map_err(|e| ApiError::Internal(e.to_string()))
    }).collect();
    let arr = jsonb::OwnedJsonb::build_array(entry_jsonbs?.iter().map(|o| o.as_raw()))
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("entries", arr)])
}

pub async fn keys(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let keys = resolved.shelf.keys(&tx).map_err(|e| ApiError::Internal(e.to_string()))?;

    let key_jsonbs: Result<Vec<_>, _> = keys.iter().map(|k| key_to_owned(k)).collect();
    let arr = jsonb::OwnedJsonb::build_array(key_jsonbs?.iter().map(|o| o.as_raw()))
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("keys", arr)])
}

pub async fn values(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let vals = resolved.shelf.values(&tx).map_err(|e| ApiError::Internal(e.to_string()))?;

    let val_jsonbs: Result<Vec<_>, _> = vals.iter().map(|v| value_to_owned(v)).collect();
    let arr = jsonb::OwnedJsonb::build_array(val_jsonbs?.iter().map(|o| o.as_raw()))
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("values", arr)])
}

pub async fn range(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let raw = parsed.as_raw();
    let start = owned_to_key(&get_field(&raw, "start")?)?;
    let end = owned_to_key(&get_field(&raw, "end")?)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let entries = resolved.shelf.get_range(&tx, &start, &end).map_err(|e| ApiError::Internal(e.to_string()))?;

    let entry_jsonbs: Result<Vec<_>, _> = entries.iter().map(|(k, v)| {
        let ko = key_to_owned(k)?;
        let vo = value_to_owned(v)?;
        jsonb::OwnedJsonb::build_array([ko.as_raw(), vo.as_raw()])
            .map_err(|e| ApiError::Internal(e.to_string()))
    }).collect();
    let arr = jsonb::OwnedJsonb::build_array(entry_jsonbs?.iter().map(|o| o.as_raw()))
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("entries", arr)])
}

pub async fn exists(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let raw = parsed.as_raw();
    let key = owned_to_key(&get_field(&raw, "key")?)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let exists = resolved.shelf.exists(&tx, &key).map_err(|e| ApiError::Internal(e.to_string()))?;

    let val = jsonb::to_owned_jsonb(&exists).map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("exists", val)])
}

pub async fn count(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let count = resolved.shelf.count(&tx).map_err(|e| ApiError::Internal(e.to_string()))?;

    let val = jsonb::to_owned_jsonb(&count).map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("count", val)])
}

pub async fn batch_set(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let entries = parse_entries(&parsed)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_write().map_err(|e| ApiError::Internal(e.to_string()))?;
    resolved.shelf.batch_set(&tx, &entries).map_err(|e| ApiError::Internal(e.to_string()))?;
    tx.commit().map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn batch_put(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let entries = parse_entries(&parsed)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_write().map_err(|e| ApiError::Internal(e.to_string()))?;
    resolved.shelf.batch_put(&tx, &entries).map_err(|e| ApiError::Internal(e.to_string()))?;
    tx.commit().map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn batch_delete(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let keys = parse_keys_from_body(&parsed)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_write().map_err(|e| ApiError::Internal(e.to_string()))?;
    resolved.shelf.batch_delete(&tx, &keys).map_err(|e| ApiError::Internal(e.to_string()))?;
    tx.commit().map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn batch_get(
    state: State<Arc<AppState>>,
    path: Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let resolved = resolve_shelf(state, path).await?;
    let parsed = parse_body(&body)?;
    let keys = parse_keys_from_body(&parsed)?;

    let db = resolved.cabinet.database();
    let tx = db.begin_read().map_err(|e| ApiError::Internal(e.to_string()))?;
    let results = resolved.shelf.get_batch(&tx, &keys).map_err(|e| ApiError::Internal(e.to_string()))?;

    let mut val_jsonbs = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        let opt = results.get(i).map_err(|e| ApiError::Internal(e.to_string()))?;
        match opt {
            Some(val) => val_jsonbs.push(value_to_owned(&val)?),
            None => val_jsonbs.push(
                jsonb::to_owned_jsonb(&()).map_err(|e| ApiError::Internal(e.to_string()))?
            ),
        }
    }
    let arr = jsonb::OwnedJsonb::build_array(val_jsonbs.iter().map(|o| o.as_raw()))
        .map_err(|e| ApiError::Internal(e.to_string()))?;
    build_response(&[("values", arr)])
}

// --- Batch parsing helpers ---

fn parse_entries(parsed: &jsonb::OwnedJsonb) -> Result<Vec<(Key, Value)>, ApiError> {
    let raw = parsed.as_raw();
    let entries_owned = get_field(&raw, "entries")?;
    let entries_raw = entries_owned.as_raw();

    let len = entries_raw.array_length()
        .map_err(|e| ApiError::JsonParse(e.to_string()))?
        .ok_or_else(|| ApiError::JsonParse("'entries' must be an array".into()))?;

    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let pair = entries_raw.get_by_index(i)
            .map_err(|e| ApiError::JsonParse(e.to_string()))?
            .ok_or_else(|| ApiError::JsonParse("missing entry".into()))?;
        let pair_raw = pair.as_raw();

        let key_owned = pair_raw.get_by_index(0)
            .map_err(|e| ApiError::JsonParse(e.to_string()))?
            .ok_or_else(|| ApiError::JsonParse("entry missing key".into()))?;
        let val_owned = pair_raw.get_by_index(1)
            .map_err(|e| ApiError::JsonParse(e.to_string()))?
            .ok_or_else(|| ApiError::JsonParse("entry missing value".into()))?;

        result.push((owned_to_key(&key_owned)?, owned_to_value(&val_owned)?));
    }
    Ok(result)
}

fn parse_keys_from_body(parsed: &jsonb::OwnedJsonb) -> Result<Vec<Key>, ApiError> {
    let raw = parsed.as_raw();
    let keys_owned = get_field(&raw, "keys")?;
    let keys_raw = keys_owned.as_raw();

    let len = keys_raw.array_length()
        .map_err(|e| ApiError::JsonParse(e.to_string()))?
        .ok_or_else(|| ApiError::JsonParse("'keys' must be an array".into()))?;

    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let item = keys_raw.get_by_index(i)
            .map_err(|e| ApiError::JsonParse(e.to_string()))?
            .ok_or_else(|| ApiError::JsonParse("missing key".into()))?;
        result.push(owned_to_key(&item)?);
    }
    Ok(result)
}
