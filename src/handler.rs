use crate::click_house::ClickHouseClient;
use async_nats::jetstream::{AckKind, Message};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::{select, time};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Clone)]
pub struct Route {
    pub table: &'static str,
    pub format_schema: &'static str,
}

pub fn route_for_subject(subject: &str) -> Option<Route> {
    match subject {
        "events.login" => Some(Route {
            table: "login_events",
            format_schema: "dto.proto:LoginEvent",
        }),
        "events.sabte_ahval" => Some(Route {
            table: "sabte_ahval_events",
            format_schema: "dto.proto:SabteAhvalEvent",
        }),
        "events.angulak.like" => Some(Route {
            table: "angulak_like_events",
            format_schema: "dto.proto:AngulakLikeEvent",
        }),
        "events.angulak.watch" => Some(Route {
            table: "angulak_watch_events",
            format_schema: "dto.proto:AngulakWatchEvent",
        }),
        "events.session" => Some(Route {
            table: "session_events",
            format_schema: "dto.proto:SessionEvent",
        }),
        "events.angulak.comment" => Some(Route {
            table: "angulak_comment_events",
            format_schema: "dto.proto:AngulakCommentEvent",
        }),
        "events.shahrefarang.item" => Some(Route {
            table: "shahrefarang_item_events",
            format_schema: "dto.proto:ShahreFarangItemEvent",
        }),
        "events.shahrefarang.play_info" => Some(Route {
            table: "shahrefarang_play_info_events",
            format_schema: "dto.proto:ShahreFarangPlayInfoEvent",
        }),
        "events.angulak.bookmark" => Some(Route {
            table: "angulak_bookmark_events",
            format_schema: "dto.proto:AngulakBookmarkEvent",
        }),
        _ => None,
    }
}

struct BatchItem {
    payload: Vec<u8>,
    msg: Message,
}

struct SubjectBatch {
    route: Route,
    rows: Vec<BatchItem>,
    bytes: usize,
}

pub struct Batcher {
    ch: ClickHouseClient,
    max_rows: usize,
    max_bytes: usize,
    flush_interval: time::Duration,

    batches: HashMap<String, SubjectBatch>,
}

impl Batcher {
    pub fn new(
        ch: ClickHouseClient,
        max_rows: usize,
        max_bytes: usize,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            ch,
            max_rows,
            max_bytes,
            flush_interval: time::Duration::from_millis(flush_interval_ms),
            batches: Default::default(),
        }
    }

    fn add(&mut self, subject: String, route: Route, payload: Vec<u8>, msg: Message) {
        let entry = self
            .batches
            .entry(subject.clone())
            .or_insert_with(|| SubjectBatch {
                route,
                rows: Vec::with_capacity(self.max_rows),
                bytes: 0,
            });
        entry.bytes += payload.len();
        entry.rows.push(BatchItem { payload, msg });
    }

    async fn flush_subject(&mut self, subject: &str) {
        if let Some(batch) = self.batches.remove(subject) {
            if batch.rows.is_empty() {
                return;
            }
            let rows_bytes: Vec<Vec<u8>> = batch.rows.iter().map(|b| b.payload.clone()).collect();
            let route = batch.route.clone();

            match self
                .ch
                .insert_protobuf_batch(route.table, route.format_schema, &rows_bytes)
                .await
            {
                Ok(_) => {
                    info!("Flushed {} rows to {}.", batch.rows.len(), route.table);
                    for item in batch.rows {
                        let _ = item.msg.ack().await;
                    }
                }
                Err(e) => {
                    error!("Flush failed for subject {}: {}", subject, e);
                    let permanent = is_permanent_ch_error(&e.to_string());
                    for item in batch.rows {
                        let _ = if permanent {
                            item.msg.ack_with(AckKind::Term).await
                        } else {
                            item.msg.ack_with(AckKind::Nak(None)).await
                        };
                    }
                }
            }
        }
    }

    async fn flush_due(&mut self) {
        let subjects: Vec<String> = self
            .batches
            .iter()
            .filter_map(|(s, b)| {
                if b.rows.len() >= self.max_rows || b.bytes >= self.max_bytes {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect();

        for s in subjects {
            self.flush_subject(&s).await;
        }
    }

    async fn flush_all(&mut self) {
        let subjects: Vec<String> = self.batches.keys().cloned().collect();
        for s in subjects {
            self.flush_subject(&s).await;
        }
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<(String, Route, Vec<u8>, Message)>,
        shutdown: CancellationToken,
    ) {
        let mut ticker = time::interval(self.flush_interval);
        loop {
            select! {
                _ = shutdown.cancelled() => {
                    info!("Batcher shutdown: flushing all.");
                    self.flush_all().await;
                    break;
                }
                _ = ticker.tick() => {
                    self.flush_due().await;
                }
                maybe_item = rx.recv() => {
                    match maybe_item {
                        Some((subject, route, payload, msg)) => {
                            self.add(subject.clone(), route.clone(), payload, msg);
                            // Re-fetch and flush if needed
                            let need_flush = {
                                let b = self.batches.get(&subject).unwrap();
                                b.rows.len() >= self.max_rows || b.bytes >= self.max_bytes
                            };
                            if need_flush {
                                self.flush_subject(&subject).await;
                            }
                        }
                        None => {
                            info!("Batcher input channel closed. Flushing all.");
                            self.flush_all().await;
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn is_permanent_ch_error(s: &str) -> bool {
    s.contains("400")
        || s.contains("404")
        || s.contains("422")
        || s.contains("Cannot parse")
        || s.contains("444")
}
