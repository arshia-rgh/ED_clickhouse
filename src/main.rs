use tracing::{debug, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

mod click_house;
mod config;
mod error;
mod nats;

#[tokio::main]
async fn main() {
    let app_configs = config::AppConfig::load_from_file("config/default.toml").unwrap();

    init_tracing(app_configs.tracing.clone());
    let nats_client = nats::Nats::new(app_configs.nats.clone()).await.unwrap();
    info!("starting up");
    debug!("starting up");
    println!("{}, {:?}", "app_configs", app_configs);
}

fn init_tracing(trace_config: config::TracingConfig) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(trace_config.level));

    let fmt_layer = fmt::layer()
        .with_level(trace_config.with_level)
        .with_target(trace_config.with_target)
        .with_thread_ids(trace_config.with_thread_ids)
        .with_line_number(trace_config.with_line_number)
        .with_file(trace_config.with_file);

    match trace_config.format {
        config::LogFormat::Json => {
            tracing_subscriber::registry()
                .with(fmt_layer.json())
                .with(env_filter)
                .init();
        }
        config::LogFormat::Text => {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(env_filter)
                .init();
        }
    }
}
