#![allow(clippy::indexing_slicing)]
//! Integration tests for GCP Pub/Sub sink.
//!
//! Uses GCP Cloud SDK emulator to test Pub/Sub message publishing.

#![cfg(feature = "sink-gcp-pubsub")]

use chrono::Utc;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use postgres_stream::sink::Sink;
use postgres_stream::sink::gcp_pubsub::{GcpPubsubSink, GcpPubsubSinkConfig};
use postgres_stream::test_utils::ensure_pubsub_emulator;
use postgres_stream::types::{EventIdentifier, PgLsn, StreamId, TriggeredEvent};
use uuid::Uuid;

/// Creates a test event with the given payload key.
fn make_test_event(key: &str) -> TriggeredEvent {
    TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "key": key, "value": "test_data" }),
        metadata: Some(serde_json::json!({ "source": "test" })),
        commit_lsn: None,
        lsn: Some(PgLsn::from(12345u64)),
    }
}

/// Creates a Pub/Sub client connected to the emulator.
async fn create_pubsub_client(emulator_host: &str, project_id: &str) -> Client {
    // Set the emulator host environment variable.
    // SAFETY: This is only called during test setup when no other threads are
    // accessing environment variables concurrently.
    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", emulator_host);
    }

    let config = ClientConfig {
        project_id: Some(project_id.to_string()),
        ..ClientConfig::default()
    };

    Client::new(config)
        .await
        .expect("Failed to create Pub/Sub client")
}

/// Creates a topic and subscription for testing.
async fn setup_topic_and_subscription(client: &Client, topic_name: &str, subscription_name: &str) {
    let topic = client.topic(topic_name);

    // Create topic if it doesn't exist.
    if !topic.exists(None).await.unwrap_or(false) {
        topic
            .create(None, None)
            .await
            .expect("Failed to create topic");
    }

    // Create subscription.
    let subscription = client.subscription(subscription_name);
    if !subscription.exists(None).await.unwrap_or(false) {
        subscription
            .create(
                topic.fully_qualified_name(),
                SubscriptionConfig::default(),
                None,
            )
            .await
            .expect("Failed to create subscription");
    }
}

/// Pulls messages from a subscription.
async fn pull_messages(client: &Client, subscription_name: &str, count: usize) -> Vec<String> {
    let subscription = client.subscription(subscription_name);
    let mut messages = Vec::new();

    // Pull messages with retries.
    for _ in 0..10 {
        let pulled = subscription.pull(10, None).await;

        if let Ok(pulled_messages) = pulled {
            for msg in pulled_messages {
                let data =
                    String::from_utf8(msg.message.data.clone()).expect("Invalid UTF-8 in message");
                messages.push(data);
                // Acknowledge the message.
                let _ = msg.ack().await;
            }
        }

        if messages.len() >= count {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    messages
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gcp_pubsub_sink_publishes_events() {
    let port = ensure_pubsub_emulator().await;
    let emulator_host = format!("localhost:{port}");
    let project_id = "test-project";
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let subscription_name = format!("test-sub-{}", Uuid::new_v4());

    // Set up topic and subscription.
    let client = create_pubsub_client(&emulator_host, project_id).await;
    setup_topic_and_subscription(&client, &topic_name, &subscription_name).await;

    // Create sink.
    let config = GcpPubsubSinkConfig {
        project_id: project_id.to_string(),
        topic: topic_name.clone(),
        emulator_host: Some(emulator_host.clone()),
    };

    let sink = GcpPubsubSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publish events.
    let events = vec![make_test_event("event1"), make_test_event("event2")];
    sink.publish_events(events)
        .await
        .expect("Failed to publish");

    // Verify messages arrived - only payload is sent.
    let messages = pull_messages(&client, &subscription_name, 2).await;
    assert_eq!(messages.len(), 2, "Expected 2 messages");

    for msg in &messages {
        let payload: serde_json::Value =
            serde_json::from_str(msg).expect("Failed to parse message");
        // Only payload fields should be present.
        assert!(payload["key"].is_string());
        assert!(payload["value"].is_string());
        // No envelope fields.
        assert!(payload.get("id").is_none());
        assert!(payload.get("metadata").is_none());
        assert!(payload.get("lsn").is_none());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gcp_pubsub_sink_handles_empty_batch() {
    let port = ensure_pubsub_emulator().await;
    let emulator_host = format!("localhost:{port}");
    let project_id = "test-project";
    let topic_name = format!("test-topic-{}", Uuid::new_v4());

    // Set the emulator host environment variable before creating the sink.
    // SAFETY: This is only called during test setup.
    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", &emulator_host);
    }

    let config = GcpPubsubSinkConfig {
        project_id: project_id.to_string(),
        topic: topic_name,
        emulator_host: Some(emulator_host),
    };

    let sink = GcpPubsubSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publishing empty batch should succeed without making any API calls.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gcp_pubsub_sink_sends_only_payload() {
    let port = ensure_pubsub_emulator().await;
    let emulator_host = format!("localhost:{port}");
    let project_id = "test-project";
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let subscription_name = format!("test-sub-{}", Uuid::new_v4());

    let client = create_pubsub_client(&emulator_host, project_id).await;
    setup_topic_and_subscription(&client, &topic_name, &subscription_name).await;

    let config = GcpPubsubSinkConfig {
        project_id: project_id.to_string(),
        topic: topic_name,
        emulator_host: Some(emulator_host),
    };

    let sink = GcpPubsubSink::new(config)
        .await
        .expect("Failed to create sink");

    // Create event with metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "action": "created", "user_id": 123 }),
        metadata: Some(serde_json::json!({ "routing_key": "orders" })),
        commit_lsn: None,
        lsn: Some(PgLsn::from(99999u64)),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    let messages = pull_messages(&client, &subscription_name, 1).await;
    assert_eq!(messages.len(), 1);

    let payload: serde_json::Value =
        serde_json::from_str(&messages[0]).expect("Failed to parse message");

    // Only payload is sent.
    assert_eq!(payload["action"], "created");
    assert_eq!(payload["user_id"], 123);
    // No envelope fields.
    assert!(payload.get("id").is_none());
    assert!(payload.get("metadata").is_none());
    assert!(payload.get("lsn").is_none());
}

#[test]
fn test_sink_name() {
    assert_eq!(GcpPubsubSink::name(), "gcp-pubsub");
}
