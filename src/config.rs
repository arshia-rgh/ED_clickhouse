use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub tracing: TracingConfig,
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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}
