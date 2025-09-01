use std::time::Duration;
use crate::config;
use async_nats::jetstream::consumer::AckPolicy;
use async_nats::jetstream::consumer::pull::{Config as PullConfig, Stream as Messages};
use async_nats::jetstream::stream;
use async_nats::{Client, ConnectOptions};
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub struct Nats {
    client: Client,
    js: async_nats::jetstream::Context,
    stream_name: String,
    subjects: Vec<String>,
    consumer_name: String,
}

impl Nats {
    pub async fn new(nats_config: config::NatsConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let client = ConnectOptions::with_user_and_password(
            nats_config.username.clone(),
            nats_config.password.clone(),
        )
        .connect(nats_config.get_addr())
        .await?;

        let js = async_nats::jetstream::new(client.clone());
        match js.get_stream(nats_config.stream_config.name.clone()).await {
            Ok(_) => {}
            Err(e) => {
                if e.to_string().contains("stream not found")
                    || nats_config.stream_config.need_create()
                {
                    info!(
                        "stream not found, creating stream: {}",
                        nats_config.stream_config.name
                    );

                    js.create_stream(stream::Config {
                        name: nats_config.stream_config.name.clone(),
                        subjects: nats_config.subjects.clone(),
                        retention: nats_config.stream_config.retention,
                        discard: nats_config.stream_config.discard,
                        storage: nats_config.stream_config.storage,
                        max_consumers: nats_config.stream_config.max_consumers as i32,
                        ..Default::default()
                    })
                    .await?;
                }
            }
        }

        Ok(Nats {
            client,
            js,
            stream_name: nats_config.stream_config.name,
            subjects: nats_config.subjects,
            consumer_name: nats_config.consumer_name,
        })
    }

    pub async fn consume(&self) -> Result<Messages, Box<dyn std::error::Error>> {
        let consumer = self
            .js
            .create_consumer_on_stream(
                PullConfig {
                    durable_name: Some(self.consumer_name.clone()),
                    filter_subjects: self.subjects.clone(),
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(120),
                    max_ack_pending: 200_000,
                    max_bytes: 5_000_000,
                    max_deliver: 3,
                    ..Default::default()
                },
                self.stream_name.clone(),
            )
            .await?;

        let stream_messages = consumer.messages().await?;
        Ok(stream_messages)
    }

    pub async fn consume_to_channel(
        &self,
        tx: mpsc::Sender<async_nats::jetstream::Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut messages = self.consume().await?;

        while let Some(next) = messages.next().await {
            match next {
                Ok(msg) => {
                    if tx.send(msg).await.is_err() {
                        warn!("receiver dropped; stopping consumer loop");
                        break;
                    }
                }
                Err(e) => warn!("failed to pull message: {e}"),
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.client.drain().await?;

        Ok(())
    }
}
