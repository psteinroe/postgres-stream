//! Integration tests for the AWS SQS sink.

#![cfg(feature = "sink-sqs")]

use postgres_stream::sink::Sink;
use postgres_stream::sink::sqs::{SqsSink, SqsSinkConfig};
use postgres_stream::test_utils::ensure_elasticmq;
use postgres_stream::types::{EventIdentifier, StreamId, TriggeredEvent};

use aws_sdk_sqs::Client;
use chrono::Utc;

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
async fn test_sqs_sink_publishes_events() {
    let elasticmq_port = ensure_elasticmq().await;
    let endpoint_url = format!("http://127.0.0.1:{elasticmq_port}");
    let region = "us-east-1";

    // Create SQS client for test setup.
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new(region))
        .endpoint_url(&endpoint_url)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "x", "x", None, None, "test",
        ))
        .load()
        .await;
    let client = Client::new(&sdk_config);

    // Create a test queue.
    let queue_name = "pgstream-test-queue";
    let create_result = client
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .expect("Failed to create queue");
    let queue_url = create_result.queue_url().expect("Queue URL not returned");

    // Create the sink.
    let config = SqsSinkConfig {
        queue_url: Some(queue_url.to_string()),
        region: region.to_string(),
        endpoint_url: Some(endpoint_url),
        access_key_id: Some("x".to_string()),
        secret_access_key: Some("x".to_string()),
    };

    let sink = SqsSink::new(config)
        .await
        .expect("Failed to create SQS sink");

    // Publish test events.
    let events = vec![
        make_test_event("sqs-event-1"),
        make_test_event("sqs-event-2"),
    ];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Receive messages to verify.
    let receive_result = client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .expect("Failed to receive messages");

    let messages = receive_result.messages();
    assert_eq!(messages.len(), 2);

    // Verify message content - only payload is sent now.
    for message in messages {
        let body = message.body().expect("Message has no body");
        let payload: serde_json::Value =
            serde_json::from_str(body).expect("Failed to parse message");

        // Payload fields from make_test_event
        assert!(payload.get("test_id").is_some());
        assert!(payload.get("message").is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sqs_sink_empty_batch() {
    let elasticmq_port = ensure_elasticmq().await;
    let endpoint_url = format!("http://127.0.0.1:{elasticmq_port}");
    let region = "us-east-1";

    // Create SQS client for test setup.
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new(region))
        .endpoint_url(&endpoint_url)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "x", "x", None, None, "test",
        ))
        .load()
        .await;
    let client = Client::new(&sdk_config);

    // Create a test queue.
    let queue_name = "pgstream-empty-queue";
    let create_result = client
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .expect("Failed to create queue");
    let queue_url = create_result.queue_url().expect("Queue URL not returned");

    let config = SqsSinkConfig {
        queue_url: Some(queue_url.to_string()),
        region: region.to_string(),
        endpoint_url: Some(endpoint_url),
        access_key_id: Some("x".to_string()),
        secret_access_key: Some("x".to_string()),
    };

    let sink = SqsSink::new(config)
        .await
        .expect("Failed to create SQS sink");

    // Empty batch should succeed without error.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sqs_sink_queue_url_from_metadata() {
    let elasticmq_port = ensure_elasticmq().await;
    let endpoint_url = format!("http://127.0.0.1:{elasticmq_port}");
    let region = "us-east-1";

    // Create SQS client for test setup.
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_sqs::config::Region::new(region))
        .endpoint_url(&endpoint_url)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "x", "x", None, None, "test",
        ))
        .load()
        .await;
    let client = Client::new(&sdk_config);

    // Create a test queue.
    let queue_name = "pgstream-metadata-queue";
    let create_result = client
        .create_queue()
        .queue_name(queue_name)
        .send()
        .await
        .expect("Failed to create queue");
    let queue_url = create_result.queue_url().expect("Queue URL not returned");

    // Create the sink with no queue URL - it must come from metadata.
    let config = SqsSinkConfig {
        queue_url: None,
        region: region.to_string(),
        endpoint_url: Some(endpoint_url),
        access_key_id: Some("x".to_string()),
        secret_access_key: Some("x".to_string()),
    };

    let sink = SqsSink::new(config)
        .await
        .expect("Failed to create SQS sink");

    // Create event with queue_url in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("metadata-queue-event".to_string(), Utc::now()),
        payload: serde_json::json!({ "test": "data" }),
        metadata: Some(serde_json::json!({
            "queue_url": queue_url
        })),
        stream_id: StreamId::from(1u64),
        lsn: None,
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish event with metadata queue URL");

    // Verify message was sent.
    let receive_result = client
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(1)
        .send()
        .await
        .expect("Failed to receive messages");

    let messages = receive_result.messages();
    assert_eq!(messages.len(), 1);
}

#[test]
fn test_sink_name() {
    assert_eq!(SqsSink::name(), "sqs");
}
