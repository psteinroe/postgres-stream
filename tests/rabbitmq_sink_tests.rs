#![allow(clippy::indexing_slicing)]
//! Integration tests for the RabbitMQ sink.

#![cfg(feature = "sink-rabbitmq")]

use postgres_stream::sink::Sink;
use postgres_stream::sink::rabbitmq::{RabbitmqSink, RabbitmqSinkConfig};
use postgres_stream::test_utils::ensure_rabbitmq;
use postgres_stream::types::{EventIdentifier, StreamId, TriggeredEvent};

use chrono::Utc;
use lapin::{Connection, ConnectionProperties, options::BasicGetOptions};

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
async fn test_rabbitmq_sink_publishes_events() {
    let rabbitmq_port = ensure_rabbitmq().await;
    let rabbitmq_url = format!("amqp://guest:guest@127.0.0.1:{rabbitmq_port}");
    let exchange = "pgstream.test.exchange";
    let routing_key = "test.events";
    let queue = "pgstream.test.queue";

    let config = RabbitmqSinkConfig {
        url: rabbitmq_url.clone(),
        exchange: Some(exchange.to_string()),
        routing_key: Some(routing_key.to_string()),
        queue: Some(queue.to_string()),
    };

    let sink = RabbitmqSink::new(config)
        .await
        .expect("Failed to create RabbitMQ sink");

    // Publish test events.
    let events = vec![
        make_test_event("rabbitmq-event-1"),
        make_test_event("rabbitmq-event-2"),
    ];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Connect and verify messages are in the queue.
    let connection = Connection::connect(&rabbitmq_url, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = connection
        .create_channel()
        .await
        .expect("Failed to create channel");

    // Get first message.
    let msg1 = channel
        .basic_get(queue, BasicGetOptions::default())
        .await
        .expect("Failed to get message")
        .expect("Expected first message");

    let payload1: serde_json::Value =
        serde_json::from_slice(&msg1.data).expect("Failed to parse first message");

    // Only payload fields should be present.
    assert!(payload1.get("test_id").is_some());
    assert!(payload1.get("message").is_some());
    // No envelope fields.
    assert!(payload1.get("id").is_none());
    assert!(payload1.get("metadata").is_none());
    assert!(payload1.get("lsn").is_none());

    // Get second message.
    let msg2 = channel
        .basic_get(queue, BasicGetOptions::default())
        .await
        .expect("Failed to get message")
        .expect("Expected second message");

    let payload2: serde_json::Value =
        serde_json::from_slice(&msg2.data).expect("Failed to parse second message");

    assert!(payload2.get("test_id").is_some());
    assert!(payload2.get("id").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rabbitmq_sink_empty_batch() {
    let rabbitmq_port = ensure_rabbitmq().await;
    let rabbitmq_url = format!("amqp://guest:guest@127.0.0.1:{rabbitmq_port}");

    let config = RabbitmqSinkConfig {
        url: rabbitmq_url,
        exchange: Some("pgstream.empty.exchange".to_string()),
        routing_key: Some("empty.events".to_string()),
        queue: None,
    };

    let sink = RabbitmqSink::new(config)
        .await
        .expect("Failed to create RabbitMQ sink");

    // Empty batch should succeed without error.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rabbitmq_sink_uses_metadata_routing() {
    let rabbitmq_port = ensure_rabbitmq().await;
    let rabbitmq_url = format!("amqp://guest:guest@127.0.0.1:{rabbitmq_port}");
    let exchange = "pgstream.metadata.exchange";
    let routing_key = "metadata.events";
    let queue = "pgstream.metadata.queue";

    // First, set up the exchange and queue using a separate connection.
    let connection = Connection::connect(&rabbitmq_url, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = connection
        .create_channel()
        .await
        .expect("Failed to create channel");

    channel
        .exchange_declare(
            exchange,
            lapin::ExchangeKind::Topic,
            lapin::options::ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .expect("Failed to declare exchange");

    channel
        .queue_declare(
            queue,
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    channel
        .queue_bind(
            queue,
            exchange,
            routing_key,
            lapin::options::QueueBindOptions::default(),
            lapin::types::FieldTable::default(),
        )
        .await
        .expect("Failed to bind queue");

    // Create sink without exchange/routing_key - will get it from metadata.
    let config = RabbitmqSinkConfig {
        url: rabbitmq_url.clone(),
        exchange: None,
        routing_key: None,
        queue: None,
    };

    let sink = RabbitmqSink::new(config)
        .await
        .expect("Failed to create RabbitMQ sink");

    // Create event with exchange and routing_key in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("rabbitmq-metadata-event".to_string(), Utc::now()),
        payload: serde_json::json!({
            "test_id": "metadata-event",
            "message": "Test event with metadata routing",
        }),
        metadata: Some(serde_json::json!({
            "exchange": exchange,
            "routing_key": routing_key
        })),
        stream_id: StreamId::from(1u64),
        commit_lsn: None,
        lsn: Some("0/16B3748".parse().unwrap()),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish events");

    // Give RabbitMQ time to route the message.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify message was received.
    let msg = channel
        .basic_get(queue, BasicGetOptions::default())
        .await
        .expect("Failed to get message")
        .expect("Expected message");

    let payload: serde_json::Value =
        serde_json::from_slice(&msg.data).expect("Failed to parse message");

    // Only payload is sent.
    assert_eq!(payload["test_id"], "metadata-event");
    assert!(payload.get("id").is_none());
    assert!(payload.get("metadata").is_none());
}

#[test]
fn test_sink_name() {
    assert_eq!(RabbitmqSink::name(), "rabbitmq");
}
