use crate::handler::Route;
use async_nats::jetstream::Message;
use async_nats::jetstream::message::AckKind;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use tokio::signal;
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

mod click_house;
mod config;
mod error;
mod handler;
mod nats;

#[tokio::main]
async fn main() {
    let app_configs = config::AppConfig::load_from_file("config/default.toml").unwrap();

    init_tracing(app_configs.tracing.clone());
    let shutdown = CancellationToken::new();
    let nats_client = nats::Nats::new(app_configs.nats.clone()).await.unwrap();
    let clickhouse_client = click_house::ClickHouseClient::new(app_configs.clickhouse.clone());
    clickhouse_client.ping().await.unwrap();
    let batcher = handler::Batcher::new(
        clickhouse_client,
        app_configs.batcher.max_rows,
        app_configs.batcher.max_bytes,
        app_configs.batcher.flush_interval_ms,
    );

    let (tx, rx) = mpsc::channel::<(String, Route, Vec<u8>, Message)>(app_configs.batcher.max_rows);
    let batcher_task = {
        let shutdown = shutdown.clone();
        tokio::spawn(batcher.run(rx, shutdown))
    };
    let messages = nats_client.consume().await.unwrap();

    let concurrency = std::env::var("WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| (n.get() * 4).clamp(8, 256))
                .unwrap_or(32)
        });

    info!("Start consuming messages..., limit {}", concurrency);

    let tx_for_processing = tx.clone();

    let processing = messages.for_each_concurrent(concurrency, |message| {
        let tx = tx_for_processing.clone();

        async move {
            let message = match message {
                Ok(msg) => msg,
                Err(e) => {
                    warn!("Error receiving message: {}", e);
                    return;
                }
            };
            // info!("Received a message: {:?}", message);

            let subject = message.subject.clone();
            let Some(route) = handler::route_for_subject(&subject) else {
                warn!("No route found for subject: {}", subject);
                let _ = message.ack_with(AckKind::Term).await;
                return;
            };

            match tx
                .send((
                    subject.into_string(),
                    route,
                    message.payload.to_vec(),
                    message,
                ))
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    warn!("Batcher channel closed; NAK message for retry.");
                    let (_, _, _, message) = err.0;
                    let _ = message.ack_with(AckKind::Nak(None)).await;
                }
            }
        }
    });

    tokio::select! {
        _ = signal::ctrl_c() => {
            shutdown.cancel();
            info!("Received Ctrl+C, shutting down...");
        }
        _ = processing => {
            info!("Message processing completed.");
        }
    }

    drop(tx);
    let _ = nats_client.close().await;
    let _ = batcher_task.await;
    info!("NATS client closed, exiting.");
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
