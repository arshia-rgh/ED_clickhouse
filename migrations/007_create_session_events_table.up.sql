CREATE TABLE IF NOT EXISTS session_events (
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
    is_ended Boolean
) ENGINE = MergeTree()
ORDER BY
    (timestamp, event_name);