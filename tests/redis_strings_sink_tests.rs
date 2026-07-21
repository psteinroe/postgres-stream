#![allow(clippy::indexing_slicing)]
//! Integration tests for the Redis Strings sink.

#![cfg(all(feature = "sink-redis-strings", feature = "test-utils"))]

use postgres_stream::sink::Sink;
use postgres_stream::sink::redis_strings::{RedisStringsSink, RedisStringsSinkConfig};
use postgres_stream::test_utils::ensure_redis;
use postgres_stream::types::{EventIdentifier, StreamId, TriggeredEvent};

use chrono::Utc;
use redis::AsyncCommands;

/// Creates a test event with the given ID.
fn make_test_event(id: &str) -> TriggeredEvent {
    TriggeredEvent {
        id: EventIdentifier::new(id.to_string(), Utc::now()),
        payload: serde_json::json!({
            "test_id": id,
            "message": format!("Test event {}", id),
        }),
        metadata: None,
        stream_id: StreamId::from(1u64),
        commit_lsn: None,
        lsn: Some("0/16B3748".parse().unwrap()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_strings_sink_publishes_events() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let config = RedisStringsSinkConfig {
        url: redis_url.clone(),
        key_prefix: None,
        connection_timeout_ms: None,
        response_timeout_ms: None,
        connection_retries: None,
        connection_max_delay_ms: None,
    };

    let sink = RedisStringsSink::new(config)
        .await
        .expect("Failed to create Redis Strings sink");

    // Publish test events.
    let events = vec![make_test_event("event-1"), make_test_event("event-2")];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Verify events are stored in Redis.
    let client = redis::Client::open(redis_url).expect("Failed to open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get connection");

    let value1: String = conn
        .get("event-1")
        .await
        .expect("Failed to get event-1 from Redis");
    let value2: String = conn
        .get("event-2")
        .await
        .expect("Failed to get event-2 from Redis");

    // Verify payload contains expected data.
    let parsed1: serde_json::Value = serde_json::from_str(&value1).expect("Failed to parse JSON");
    let parsed2: serde_json::Value = serde_json::from_str(&value2).expect("Failed to parse JSON");

    assert_eq!(parsed1["test_id"], "event-1");
    assert_eq!(parsed2["test_id"], "event-2");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_strings_sink_with_key_prefix() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let config = RedisStringsSinkConfig {
        url: redis_url.clone(),
        key_prefix: Some("pgstream".to_string()),
        connection_timeout_ms: None,
        response_timeout_ms: None,
        connection_retries: None,
        connection_max_delay_ms: None,
    };

    let sink = RedisStringsSink::new(config)
        .await
        .expect("Failed to create Redis Strings sink");

    // Publish test event.
    let events = vec![make_test_event("prefixed-event")];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Verify event is stored with prefix.
    let client = redis::Client::open(redis_url).expect("Failed to open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get connection");

    let value: String = conn
        .get("pgstream:prefixed-event")
        .await
        .expect("Failed to get prefixed event from Redis");

    let parsed: serde_json::Value = serde_json::from_str(&value).expect("Failed to parse JSON");
    assert_eq!(parsed["test_id"], "prefixed-event");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_strings_sink_empty_batch() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let config = RedisStringsSinkConfig {
        url: redis_url,
        key_prefix: None,
        connection_timeout_ms: None,
        response_timeout_ms: None,
        connection_retries: None,
        connection_max_delay_ms: None,
    };

    let sink = RedisStringsSink::new(config)
        .await
        .expect("Failed to create Redis Strings sink");

    // Empty batch should succeed without error.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_strings_sink_uses_key_from_metadata() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let config = RedisStringsSinkConfig {
        url: redis_url.clone(),
        key_prefix: None,
        connection_timeout_ms: None,
        response_timeout_ms: None,
        connection_retries: None,
        connection_max_delay_ms: None,
    };

    let sink = RedisStringsSink::new(config)
        .await
        .expect("Failed to create Redis Strings sink");

    // Create event with custom key in metadata.
    let custom_key = "user:12345:profile";
    let event = TriggeredEvent {
        id: EventIdentifier::new("event-with-metadata-key".to_string(), Utc::now()),
        payload: serde_json::json!({
            "test_id": "metadata-key-event",
            "message": "Test event with metadata key",
        }),
        metadata: Some(serde_json::json!({ "key": custom_key })),
        stream_id: StreamId::from(1u64),
        commit_lsn: None,
        lsn: Some("0/16B3748".parse().unwrap()),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish events");

    // Verify event is stored with metadata key, not event ID.
    let client = redis::Client::open(redis_url).expect("Failed to open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get connection");

    let value: String = conn
        .get(custom_key)
        .await
        .expect("Failed to get event from Redis");

    let parsed: serde_json::Value = serde_json::from_str(&value).expect("Failed to parse JSON");
    assert_eq!(parsed["test_id"], "metadata-key-event");
}

#[test]
fn test_sink_name() {
    assert_eq!(RedisStringsSink::name(), "redis-strings");
}
