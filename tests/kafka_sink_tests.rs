#![allow(clippy::indexing_slicing)]
//! Integration tests for the Kafka sink.

#![cfg(feature = "sink-kafka")]

use postgres_stream::sink::Sink;
use postgres_stream::sink::kafka::{KafkaSink, KafkaSinkConfig};
use postgres_stream::test_utils::ensure_kafka;
use postgres_stream::types::{EventIdentifier, StreamId, TriggeredEvent};

use chrono::Utc;
use futures::StreamExt;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::time::Duration;

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
async fn test_kafka_sink_publishes_events() {
    let kafka_port = ensure_kafka().await;
    let brokers = format!("127.0.0.1:{kafka_port}");
    let topic = "pgstream.test.topic";

    let config = KafkaSinkConfig {
        brokers: brokers.clone(),
        topic: Some(topic.to_string()),
        sasl_username: None,
        sasl_password: None,
        sasl_mechanism: None,
        security_protocol: None,
        delivery_timeout_ms: 10000,
    };

    let sink = KafkaSink::new(config).expect("Failed to create Kafka sink");

    // Publish test events.
    let events = vec![
        make_test_event("kafka-event-1"),
        make_test_event("kafka-event-2"),
    ];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Create a consumer to verify messages.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "test-consumer-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    // Consume messages with timeout.
    let mut message_stream = consumer.stream();
    let mut received_count = 0;

    let timeout = tokio::time::timeout(Duration::from_secs(30), async {
        while received_count < 2 {
            if let Some(result) = message_stream.next().await {
                let message = result.expect("Failed to receive message");
                let payload_bytes = message.payload().expect("Message has no payload");
                let payload: serde_json::Value =
                    serde_json::from_slice(payload_bytes).expect("Failed to parse message");

                // Only payload fields should be present.
                assert!(payload.get("test_id").is_some());
                assert!(payload.get("message").is_some());
                // No envelope fields.
                assert!(payload.get("id").is_none());
                assert!(payload.get("metadata").is_none());
                assert!(payload.get("lsn").is_none());

                received_count += 1;
            }
        }
    });

    timeout.await.expect("Timeout waiting for messages");
    assert_eq!(received_count, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_sink_empty_batch() {
    let kafka_port = ensure_kafka().await;
    let brokers = format!("127.0.0.1:{kafka_port}");

    let config = KafkaSinkConfig {
        brokers,
        topic: Some("pgstream.empty.topic".to_string()),
        sasl_username: None,
        sasl_password: None,
        sasl_mechanism: None,
        security_protocol: None,
        delivery_timeout_ms: 5000,
    };

    let sink = KafkaSink::new(config).expect("Failed to create Kafka sink");

    // Empty batch should succeed without error.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kafka_sink_uses_topic_from_metadata() {
    let kafka_port = ensure_kafka().await;
    let brokers = format!("127.0.0.1:{kafka_port}");
    let topic = "pgstream.metadata.topic";

    // Create sink without topic - will get it from metadata.
    let config = KafkaSinkConfig {
        brokers: brokers.clone(),
        topic: None,
        sasl_username: None,
        sasl_password: None,
        sasl_mechanism: None,
        security_protocol: None,
        delivery_timeout_ms: 10000,
    };

    let sink = KafkaSink::new(config).expect("Failed to create Kafka sink");

    // Create event with topic in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("kafka-metadata-event".to_string(), Utc::now()),
        payload: serde_json::json!({
            "test_id": "metadata-event",
            "message": "Test event with metadata topic",
        }),
        metadata: Some(serde_json::json!({ "topic": topic })),
        stream_id: StreamId::from(1u64),
        commit_lsn: None,
        lsn: Some("0/16B3748".parse().unwrap()),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish events");

    // Create a consumer to verify messages.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "test-consumer-group-metadata")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    // Consume messages with timeout.
    let mut message_stream = consumer.stream();

    let timeout = tokio::time::timeout(Duration::from_secs(30), async {
        if let Some(result) = message_stream.next().await {
            let message = result.expect("Failed to receive message");
            let payload_bytes = message.payload().expect("Message has no payload");
            let payload: serde_json::Value =
                serde_json::from_slice(payload_bytes).expect("Failed to parse message");

            // Only payload fields should be present.
            assert_eq!(payload["test_id"], "metadata-event");
            assert!(payload.get("id").is_none());
            assert!(payload.get("metadata").is_none());
        }
    });

    timeout.await.expect("Timeout waiting for messages");
}

#[test]
fn test_sink_name() {
    assert_eq!(KafkaSink::name(), "kafka");
}
