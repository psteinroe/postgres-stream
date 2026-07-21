#![allow(clippy::indexing_slicing)]
//! Integration tests for the Redis Streams sink.

#![cfg(feature = "sink-redis-streams")]

use postgres_stream::sink::Sink;
use postgres_stream::sink::redis_streams::{RedisStreamsSink, RedisStreamsSinkConfig};
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
        metadata: Some(serde_json::json!({ "source": "test" })),
        stream_id: StreamId::from(1u64),
        commit_lsn: None,
        lsn: Some("0/16B3748".parse().unwrap()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_streams_sink_publishes_events() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");
    let stream_name = "pgstream:test-events";

    let config = RedisStreamsSinkConfig {
        url: redis_url.clone(),
        stream_name: Some(stream_name.to_string()),
        max_len: None,
    };

    let sink = RedisStreamsSink::new(config)
        .await
        .expect("Failed to create Redis Streams sink");

    // Publish test events.
    let events = vec![
        make_test_event("stream-event-1"),
        make_test_event("stream-event-2"),
    ];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Verify events are in the stream.
    let client = redis::Client::open(redis_url).expect("Failed to open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get connection");

    // Read all entries from the stream.
    let result: redis::streams::StreamReadReply = conn
        .xread(&[stream_name], &["0"])
        .await
        .expect("Failed to read stream");

    assert_eq!(result.keys.len(), 1);
    let entries = &result.keys[0].ids;
    assert_eq!(entries.len(), 2);

    // Verify first entry has only payload field.
    let first_entry = &entries[0];
    assert!(first_entry.map.contains_key("payload"));
    // No envelope fields.
    assert!(!first_entry.map.contains_key("id"));
    assert!(!first_entry.map.contains_key("stream_id"));
    assert!(!first_entry.map.contains_key("created_at"));
    assert!(!first_entry.map.contains_key("metadata"));
    assert!(!first_entry.map.contains_key("lsn"));

    // Verify payload content.
    let payload_str: String =
        redis::from_redis_value(first_entry.map.get("payload").unwrap()).unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&payload_str).expect("Failed to parse payload JSON");
    assert!(payload.get("test_id").is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_streams_sink_with_max_len() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");
    let stream_name = "pgstream:maxlen-test";

    let config = RedisStreamsSinkConfig {
        url: redis_url.clone(),
        stream_name: Some(stream_name.to_string()),
        max_len: Some(5),
    };

    let sink = RedisStreamsSink::new(config)
        .await
        .expect("Failed to create Redis Streams sink");

    // Publish 10 events; stream should be trimmed to ~5.
    for i in 0..10 {
        let events = vec![make_test_event(&format!("maxlen-event-{i}"))];
        sink.publish_events(events)
            .await
            .expect("Failed to publish events");
    }

    // Verify stream length is approximately max_len (MAXLEN ~ is approximate).
    let client = redis::Client::open(redis_url).expect("Failed to open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get connection");

    let len: usize = conn
        .xlen(stream_name)
        .await
        .expect("Failed to get stream length");

    // Approximate trimming means length should be <= 10 and close to 5.
    assert!(len <= 10, "Stream length should be trimmed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_streams_sink_empty_batch() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let config = RedisStreamsSinkConfig {
        url: redis_url,
        stream_name: Some("pgstream:empty-test".to_string()),
        max_len: None,
    };

    let sink = RedisStreamsSink::new(config)
        .await
        .expect("Failed to create Redis Streams sink");

    // Empty batch should succeed without error.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_redis_streams_sink_uses_stream_from_metadata() {
    let redis_port = ensure_redis().await;
    let redis_url = format!("redis://127.0.0.1:{redis_port}");
    let stream_name = "pgstream:metadata-stream";

    // Create sink without stream_name - will get it from metadata.
    let config = RedisStreamsSinkConfig {
        url: redis_url.clone(),
        stream_name: None,
        max_len: None,
    };

    let sink = RedisStreamsSink::new(config)
        .await
        .expect("Failed to create Redis Streams sink");

    // Create event with stream in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("metadata-stream-event".to_string(), Utc::now()),
        payload: serde_json::json!({
            "test_id": "metadata-stream-event",
            "message": "Test event with metadata stream",
        }),
        metadata: Some(serde_json::json!({ "stream": stream_name })),
        stream_id: StreamId::from(1u64),
        commit_lsn: None,
        lsn: Some("0/16B3748".parse().unwrap()),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish events");

    // Verify event is in the metadata-specified stream.
    let client = redis::Client::open(redis_url).expect("Failed to open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to get connection");

    let result: redis::streams::StreamReadReply = conn
        .xread(&[stream_name], &["0"])
        .await
        .expect("Failed to read stream");

    assert_eq!(result.keys.len(), 1);
    let entries = &result.keys[0].ids;
    assert_eq!(entries.len(), 1);

    // Verify payload content.
    let first_entry = &entries[0];
    let payload_str: String =
        redis::from_redis_value(first_entry.map.get("payload").unwrap()).unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&payload_str).expect("Failed to parse payload JSON");
    assert_eq!(payload["test_id"], "metadata-stream-event");
}

#[test]
fn test_sink_name() {
    assert_eq!(RedisStreamsSink::name(), "redis-streams");
}
