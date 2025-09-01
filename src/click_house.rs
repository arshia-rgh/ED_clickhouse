use crate::config;
use tracing::info;

pub struct ClickHouseClient {
    base_url: String,
    db: String,
    user: Option<String>,
    pass: Option<String>,
    http: reqwest::Client,
}

impl ClickHouseClient {
    pub fn new(clickhouse_config: config::ClickHouseConfig) -> Self {
        let base_url = format!(
            "http://{}:{}/",
            clickhouse_config.host, clickhouse_config.port
        );
        let http = reqwest::Client::builder()
            // .http2_prior_knowledge()
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(3))
            .timeout(std::time::Duration::from_secs(30))
            .danger_accept_invalid_certs(false)
            .build()
            .expect("reqwest client");

        info!("ClickHouse HTTP endpoint: {}", base_url);

        Self {
            base_url,
            db: clickhouse_config.database,
            user: if clickhouse_config.user.is_empty() {
                None
            } else {
                Some(clickhouse_config.user)
            },
            pass: if clickhouse_config.password.is_empty() {
                None
            } else {
                Some(clickhouse_config.password)
            },
            http,
        }
    }

    pub async fn ping(&self) -> Result<(), anyhow::Error> {
        let mut req = self.http.get(format!("{}ping", self.base_url));
        if let Some(u) = &self.user {
            req = req.basic_auth(u, self.pass.clone());
        }
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() && text.trim() == "Ok." {
            Ok(())
        } else {
            Err(anyhow::anyhow!("CH ping failed {}: {}", status, text))
        }
    }

    pub async fn insert_protobuf_batch(
        &self,
        table: &str,
        format_schema: &str,
        rows: &[Vec<u8>],
    ) -> Result<(), anyhow::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(rows.iter().map(|r| r.len()).sum());
        for r in rows {
            body.extend_from_slice(r);
        }

        let query = format!(
            "INSERT INTO {}.{} FORMAT Protobuf SETTINGS format_schema='{}'",
            self.db, table, format_schema
        );

        let mut req = self.http.post(&self.base_url).query(&[("query", query)]);
        if let Some(u) = &self.user {
            req = req.basic_auth(u, self.pass.clone());
        }
        let resp = req.body(body).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!("CH insert failed {}: {}", status, text))
        }
    }
}
