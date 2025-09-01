use async_nats::jetstream::stream;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub tracing: TracingConfig,
    pub nats: NatsConfig,
}

impl AppConfig {
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&config_str)?;
        Ok(config)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TracingConfig {
    pub level: String,
    pub format: LogFormat,
    pub with_level: bool,
    pub with_target: bool,
    pub with_thread_ids: bool,
    pub with_line_number: bool,
    pub with_file: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    pub client_port: u16,
    pub server_port: u16,
    pub username: String,
    pub password: String,
    pub host: String,
    pub queue: String,
    pub subject: String,
    pub consumer_name: String,
    pub stream_config: NatsStreamConfig,
}

impl NatsConfig {
    pub fn get_addr(&self) -> String {
        format!("nats://{}:{}", self.host, self.client_port)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NatsStreamConfig {
    pub name: String,
    #[serde(with = "RetentionPolicyDef")]
    pub retention: stream::RetentionPolicy,
    #[serde(with = "DiscardPolicyDef")]
    pub discard: stream::DiscardPolicy,
    #[serde(with = "StorageTypeDef")]
    pub storage: stream::StorageType,
    pub no_ack: bool,
    pub max_consumers: u32,
    pub max_age: String,

    need_create: bool,
}

impl NatsStreamConfig {
    pub fn need_create(&self) -> bool {
        self.need_create
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(remote = "stream::RetentionPolicy", rename_all = "lowercase")]
pub enum RetentionPolicyDef {
    Limits,
    Interest,
    WorkQueue,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(remote = "stream::DiscardPolicy", rename_all = "lowercase")]
pub enum DiscardPolicyDef {
    Old,
    New,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(remote = "stream::StorageType", rename_all = "lowercase")]
pub enum StorageTypeDef {
    Memory,
    File,
}
