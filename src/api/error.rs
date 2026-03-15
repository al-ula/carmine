use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use carmine_core::key::KeyType;
use carmine_core::value::ValueType;

#[derive(Debug)]
pub enum ApiError {
    CabinetNotFound(String),
    ShelfNotFound(String),
    CabinetAlreadyExists(String),
    ShelfAlreadyExists(String),
    KeyTypeMismatch {
        expected: KeyType,
        actual: KeyType,
    },
    ValueTypeMismatch {
        expected: ValueType,
        actual: ValueType,
    },
    JsonParse(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::CabinetNotFound(name) => (
                StatusCode::NOT_FOUND,
                format!("Cabinet '{}' not found", name),
            ),
            ApiError::ShelfNotFound(name) => {
                (StatusCode::NOT_FOUND, format!("Shelf '{}' not found", name))
            }
            ApiError::CabinetAlreadyExists(name) => (
                StatusCode::CONFLICT,
                format!("Cabinet '{}' already exists", name),
            ),
            ApiError::ShelfAlreadyExists(name) => (
                StatusCode::CONFLICT,
                format!("Shelf '{}' already exists", name),
            ),
            ApiError::KeyTypeMismatch { expected, actual } => (
                StatusCode::BAD_REQUEST,
                format!(
                    "Key type mismatch: expected {:?}, got {:?}",
                    expected, actual
                ),
            ),
            ApiError::ValueTypeMismatch { expected, actual } => (
                StatusCode::BAD_REQUEST,
                format!(
                    "Value type mismatch: expected {:?}, got {:?}",
                    expected, actual
                ),
            ),
            ApiError::JsonParse(e) => (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e)),
            ApiError::Internal(e) => (StatusCode::INTERNAL_SERVER_ERROR, e),
        };

        let body = format!(r#"{{"error":"{}"}}"#, message.replace('"', r#"\""#));

        (status, [(axum::http::header::CONTENT_TYPE, "application/json")], body).into_response()
    }
}
