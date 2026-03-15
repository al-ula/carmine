use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Parser, Debug, Clone)]
#[command(name = "carmine", about = "A key-value store for the web")]
pub struct CliArgs {
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[arg(long)]
    pub no_config: bool,

    #[arg(short, long, env = "CARMINE_DATA_DIR", value_name = "DIR")]
    pub data_dir: Option<PathBuf>,

    #[arg(short, long, env = "CARMINE_BIND", value_name = "ADDR")]
    pub bind: Option<String>,

    #[arg(long, env = "CARMINE_CABINET_CACHE_SIZE", value_name = "SIZE")]
    pub cabinet_cache: Option<usize>,

    #[arg(long, env = "CARMINE_SYSTEM_CACHE_SIZE", value_name = "SIZE")]
    pub system_cache: Option<usize>,

    #[arg(long, env = "CARMINE_DURABILITY", value_name = "MODE")]
    pub durability: Option<String>,

    #[arg(short, long, env = "CARMINE_LOG_LEVEL", value_name = "LEVEL")]
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub durability: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub cabinet_size: usize,
    pub system_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ConfigFile {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub cache: CacheConfig,
    pub logging: LoggingConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:3000".into(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            durability: "immediate".into(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cabinet_size: 64 * 1024 * 1024,
            system_size: 8 * 1024 * 1024,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind: String,
    pub cabinet_cache_size: usize,
    pub system_cache_size: usize,
    pub durability: Durability,
    pub log_level: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Durability {
    Immediate,
    None,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let cli = CliArgs::parse();

        let file = if cli.no_config {
            None
        } else {
            let path = cli.config.clone().or_else(find_config_file);
            path.map(|p| load_config_file(&p)).transpose()?
        };

        let file = file.unwrap_or_default();

        Ok(Self::merge(cli, file))
    }

    fn merge(cli: CliArgs, file: ConfigFile) -> Self {
        Self {
            data_dir: cli.data_dir.unwrap_or(file.storage.data_dir),
            bind: cli.bind.unwrap_or(file.server.bind),
            cabinet_cache_size: cli.cabinet_cache.unwrap_or(file.cache.cabinet_size),
            system_cache_size: cli.system_cache.unwrap_or(file.cache.system_size),
            durability: parse_durability(&cli.durability.unwrap_or(file.storage.durability)),
            log_level: cli.log_level.unwrap_or(file.logging.level),
        }
    }

    pub fn redb_durability(&self) -> redb::Durability {
        match self.durability {
            Durability::Immediate => redb::Durability::Immediate,
            Durability::None => redb::Durability::None,
        }
    }
}

fn parse_durability(s: &str) -> Durability {
    match s.to_lowercase().as_str() {
        "none" => Durability::None,
        _ => Durability::Immediate,
    }
}

fn find_config_file() -> Option<PathBuf> {
    let candidates = ["carmine.toml", "config.toml", ".carmine.toml"];
    for name in candidates {
        let path = PathBuf::from(name);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

fn load_config_file(path: &PathBuf) -> Result<ConfigFile, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.clone(), e))?;
    toml_edit::de::from_str(&content).map_err(|e| ConfigError::Parse(path.clone(), e.to_string()))
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file {0}: {1}")]
    Read(PathBuf, std::io::Error),
    #[error("Failed to parse config file {0}: {1}")]
    Parse(PathBuf, String),
}
