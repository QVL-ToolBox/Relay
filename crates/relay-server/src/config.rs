use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::auth::AuthConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_tcp_addr")]
    pub tcp_addr: SocketAddr,
    #[serde(default = "default_ws_addr")]
    pub ws_addr: SocketAddr,
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default = "default_max_delivery_attempts")]
    pub max_delivery_attempts: u32,
    #[serde(default = "default_retry_base_secs")]
    pub retry_base_secs: u64,
    #[serde(default = "default_retry_max_secs")]
    pub retry_max_secs: u64,
    #[serde(default = "default_event_log_max")]
    pub event_log_max: u64,
    #[serde(default)]
    pub tls_addr: Option<SocketAddr>,
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    #[serde(default)]
    pub http_addr: Option<SocketAddr>,
    pub auth: AuthConfig,
}

fn default_tcp_addr() -> SocketAddr {
    "0.0.0.0:1883".parse().unwrap()
}
fn default_ws_addr() -> SocketAddr {
    "0.0.0.0:8083".parse().unwrap()
}
fn default_max_delivery_attempts() -> u32 {
    5
}
fn default_retry_base_secs() -> u64 {
    5
}
fn default_retry_max_secs() -> u64 {
    60
}
fn default_event_log_max() -> u64 {
    100_000
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ConfigError::Missing(path.to_path_buf())
            } else {
                ConfigError::Io(e)
            }
        })?;
        Ok(toml::from_str(&text)?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config file not found: {0} (an [auth] section is mandatory)")]
    Missing(PathBuf),
    #[error("failed to read config file: {0}")]
    Io(std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
}
