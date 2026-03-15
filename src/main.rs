use std::path::PathBuf;
use std::sync::Arc;

use axum::{Router, routing::get};
use dashmap::DashMap;
use tokio::net::TcpListener;

use carmine_core::{system_store::SystemStore, cabinet::Cabinet};

mod api;
mod config;

use config::Config;

pub struct AppState {
    pub system_store: SystemStore,
    pub data_dir: PathBuf,
    pub cabinets: DashMap<u64, Cabinet>,
    pub cabinet_cache_size: usize,
    pub durability: redb::Durability,
}

impl AppState {
    pub fn new(
        system_store: SystemStore,
        data_dir: PathBuf,
        cabinet_cache_size: usize,
        durability: redb::Durability,
    ) -> Self {
        Self {
            system_store,
            data_dir,
            cabinets: DashMap::new(),
            cabinet_cache_size,
            durability,
        }
    }

    pub fn get_or_open_cabinet(
        &self,
        id: u64,
        name: String,
        path: PathBuf,
    ) -> Result<Cabinet, Box<dyn std::error::Error>> {
        if let Some(cabinet) = self.cabinets.get(&id) {
            return Ok(cabinet.clone());
        }
        let cabinet = Cabinet::open(id, name, path, self.cabinet_cache_size)?;
        self.cabinets.insert(id, cabinet.clone());
        Ok(cabinet)
    }
}

#[tokio::main]
async fn main() {
    let config = Config::load().expect("Failed to load configuration");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level)),
        )
        .init();

    std::fs::create_dir_all(&config.data_dir).unwrap();

    let system_store = SystemStore::open(
        &config.data_dir.join("system.redb"),
        config.system_cache_size,
    )
    .expect("Failed to open system store");

    let state = Arc::new(AppState::new(
        system_store,
        config.data_dir.clone(),
        config.cabinet_cache_size,
        config.redb_durability(),
    ));

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .nest("/system", api::system_router())
        .nest("/v1/:cabinet/:shelf", api::normal_router())
        .with_state(state);

    let listener = TcpListener::bind(&config.bind).await.unwrap();
    tracing::info!("Listening on {}", config.bind);
    axum::serve(listener, app).await.unwrap();
}
