//! Integration tests for the NATS sink.

#![cfg(feature = "sink-nats")]

use postgres_stream::sink::Sink;
use postgres_stream::sink::nats::{NatsSink, NatsSinkConfig};
use postgres_stream::test_utils::ensure_nats;
use postgres_stream::types::{EventIdentifier, StreamId, TriggeredEvent};

use chrono::Utc;
use futures::StreamExt;

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
        lsn: Some("0/16B3748".parse().unwrap()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nats_sink_publishes_events() {
    let nats_port = ensure_nats().await;
    let nats_url = format!("nats://127.0.0.1:{nats_port}");
    let subject = "pgstream.test-events";

    let config = NatsSinkConfig {
        url: nats_url.clone(),
        subject: Some(subject.to_string()),
    };

    let sink = NatsSink::new(config)
        .await
        .expect("Failed to create NATS sink");

    // Subscribe before publishing to verify messages.
    let client = async_nats::connect(&nats_url)
        .await
        .expect("Failed to connect to NATS");

    let mut subscriber = client
        .subscribe(subject.to_string())
        .await
        .expect("Failed to subscribe");

    // Give the subscription time to fully establish.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish test events.
    let events = vec![
        make_test_event("nats-event-1"),
        make_test_event("nats-event-2"),
    ];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Verify events were received.
    let msg1 = tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.next())
        .await
        .expect("Timeout waiting for first message")
        .expect("Expected first message");

    let msg2 = tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.next())
        .await
        .expect("Timeout waiting for second message")
        .expect("Expected second message");

    // Parse and verify message content - only payload is sent.
    let payload1: serde_json::Value =
        serde_json::from_slice(&msg1.payload).expect("Failed to parse first message");
    let payload2: serde_json::Value =
        serde_json::from_slice(&msg2.payload).expect("Failed to parse second message");

    // Only payload fields should be present.
    assert!(payload1.get("test_id").is_some());
    assert!(payload1.get("message").is_some());
    assert!(payload2.get("test_id").is_some());
    assert!(payload2.get("message").is_some());
    // No envelope fields.
    assert!(payload1.get("id").is_none());
    assert!(payload1.get("metadata").is_none());
    assert!(payload1.get("lsn").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nats_sink_empty_batch() {
    let nats_port = ensure_nats().await;
    let nats_url = format!("nats://127.0.0.1:{nats_port}");

    let config = NatsSinkConfig {
        url: nats_url,
        subject: Some("pgstream.empty-test".to_string()),
    };

    let sink = NatsSink::new(config)
        .await
        .expect("Failed to create NATS sink");

    // Empty batch should succeed without error.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nats_sink_uses_topic_from_metadata() {
    let nats_port = ensure_nats().await;
    let nats_url = format!("nats://127.0.0.1:{nats_port}");
    let subject = "pgstream.metadata-topic";

    // Create sink without subject - will get it from metadata.
    let config = NatsSinkConfig {
        url: nats_url.clone(),
        subject: None,
    };

    let sink = NatsSink::new(config)
        .await
        .expect("Failed to create NATS sink");

    // Subscribe before publishing to verify messages.
    let client = async_nats::connect(&nats_url)
        .await
        .expect("Failed to connect to NATS");

    let mut subscriber = client
        .subscribe(subject.to_string())
        .await
        .expect("Failed to subscribe");

    // Give the subscription time to fully establish.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Create event with topic in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("nats-metadata-event".to_string(), Utc::now()),
        payload: serde_json::json!({
            "test_id": "metadata-event",
            "message": "Test event with metadata topic",
        }),
        metadata: Some(serde_json::json!({ "topic": subject })),
        stream_id: StreamId::from(1u64),
        lsn: Some("0/16B3748".parse().unwrap()),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish events");

    // Verify event was received.
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.next())
        .await
        .expect("Timeout waiting for message")
        .expect("Expected message");

    // Parse and verify message content - only payload is sent.
    let payload: serde_json::Value =
        serde_json::from_slice(&msg.payload).expect("Failed to parse message");

    assert_eq!(payload["test_id"], "metadata-event");
    assert!(payload.get("id").is_none());
    assert!(payload.get("metadata").is_none());
}

#[test]
fn test_sink_name() {
    assert_eq!(NatsSink::name(), "nats");
}
