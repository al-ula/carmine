use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};

use crate::AppState;

mod error;
mod extractors;
mod normal;
mod system;

pub use error::ApiError;

pub fn system_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/cabinets", post(system::create_cabinet).get(system::list_cabinets))
        .route("/cabinets/:name", get(system::get_cabinet).delete(system::delete_cabinet))
        .route("/cabinets/:name/clean", post(system::clean_cabinet))
        .route("/cabinets/:name/shelves", post(system::create_shelf).get(system::list_shelves))
        .route("/cabinets/:name/shelves/:shelf", delete(system::delete_shelf))
}

pub fn normal_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/get", post(normal::get))
        .route("/set", post(normal::set))
        .route("/put", post(normal::put))
        .route("/delete", post(normal::delete))
        .route("/all", get(normal::all))
        .route("/keys", get(normal::keys))
        .route("/values", get(normal::values))
        .route("/range", post(normal::range))
        .route("/exists", post(normal::exists))
        .route("/count", get(normal::count))
        .route("/batch/set", post(normal::batch_set))
        .route("/batch/put", post(normal::batch_put))
        .route("/batch/delete", post(normal::batch_delete))
        .route("/batch/get", post(normal::batch_get))
}
