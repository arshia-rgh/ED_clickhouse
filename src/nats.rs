use crate::config;
use async_nats::Client;
use async_nats::jetstream::stream;
use tracing::info;

pub struct Nats {
    client: Client,
    js: async_nats::jetstream::Context,
}

impl Nats {
    pub async fn new(nats_config: config::NatsConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let client = async_nats::connect(nats_config.get_addr()).await?;

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
                        name: nats_config.stream_config.name,
                        subjects: vec![nats_config.subject.clone()],
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

        Ok(Nats { client, js })
    }

    pub async fn consume() {}
}
