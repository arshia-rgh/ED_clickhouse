CREATE TABLE IF NOT EXISTS shahre_farang_play_info_events (
    event_id String,
    event_name String,
    user_id String,
    session_id String,
    anonymous_id String,
    timestamp DateTime,
    service_origin String,
    platform String,
    platform_version String,
    os_name String,
    os_version String,
    browser_name String,
    browser_version String,
    device_type String,
    screen_resolution String,
    user_agent String,
    duration Int32,
    item_id String,
    genres Array(String),
    categories Array(String),
    labels Array(String),
    has_subtitle Boolean,
    is_dubbed Boolean,
    reach_method String
) ENGINE = MergeTree()
ORDER BY
    (timestamp, event_name);