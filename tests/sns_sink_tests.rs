#![allow(clippy::indexing_slicing)]
//! Integration tests for AWS SNS sink.
//!
//! Uses LocalStack container to test SNS message publishing.
//! Verifies messages by subscribing an SQS queue to the SNS topic.

#![cfg(feature = "sink-sns")]

use chrono::Utc;
use postgres_stream::sink::Sink;
use postgres_stream::sink::sns::{SnsSink, SnsSinkConfig};
use postgres_stream::test_utils::ensure_localstack;
use postgres_stream::types::{EventIdentifier, PgLsn, StreamId, TriggeredEvent};
use uuid::Uuid;

/// Creates a test event with the given payload key.
fn make_test_event(key: &str) -> TriggeredEvent {
    TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "key": key, "value": "test_data" }),
        metadata: Some(serde_json::json!({ "source": "test" })),
        lsn: Some(PgLsn::from(12345u64)),
    }
}

/// Creates an SNS topic in LocalStack and returns the topic ARN.
async fn create_sns_topic(endpoint: &str) -> String {
    let region = aws_sdk_sns::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_sns::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_sns::Client::new(&sdk_config);

    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let result = client
        .create_topic()
        .name(&topic_name)
        .send()
        .await
        .expect("Failed to create SNS topic");

    result.topic_arn().unwrap().to_string()
}

/// Creates an SQS queue in LocalStack and returns the queue URL.
async fn create_sqs_queue(endpoint: &str) -> String {
    let region = aws_sdk_sqs::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_sqs::Client::new(&sdk_config);

    let queue_name = format!("test-queue-{}", Uuid::new_v4());
    let result = client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("Failed to create SQS queue");

    result.queue_url().unwrap().to_string()
}

/// Gets the ARN of an SQS queue.
async fn get_queue_arn(endpoint: &str, queue_url: &str) -> String {
    let region = aws_sdk_sqs::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_sqs::Client::new(&sdk_config);

    let attrs = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .expect("Failed to get queue attributes");

    attrs
        .attributes()
        .expect("No attributes returned")
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .cloned()
        .expect("Queue ARN not found")
}

/// Subscribes an SQS queue to an SNS topic.
async fn subscribe_sqs_to_sns(endpoint: &str, topic_arn: &str, queue_arn: &str) {
    let region = aws_sdk_sns::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_sns::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_sns::Client::new(&sdk_config);

    client
        .subscribe()
        .topic_arn(topic_arn)
        .protocol("sqs")
        .endpoint(queue_arn)
        .send()
        .await
        .expect("Failed to subscribe SQS to SNS");
}

/// Receives messages from an SQS queue.
async fn receive_sqs_messages(endpoint: &str, queue_url: &str, count: usize) -> Vec<String> {
    let region = aws_sdk_sqs::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_sqs::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_sqs::Client::new(&sdk_config);

    let mut messages = Vec::new();

    // Poll for messages with retries.
    for _ in 0..10 {
        let result = client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .expect("Failed to receive messages");

        for msg in result.messages() {
            if let Some(body) = msg.body() {
                messages.push(body.to_string());
            }
        }

        if messages.len() >= count {
            break;
        }
    }

    messages
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sns_sink_publishes_events() {
    let port = ensure_localstack().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    // Create SNS topic.
    let topic_arn = create_sns_topic(&endpoint).await;

    // Create SQS queue and subscribe it to the SNS topic.
    let queue_url = create_sqs_queue(&endpoint).await;
    let queue_arn = get_queue_arn(&endpoint, &queue_url).await;
    subscribe_sqs_to_sns(&endpoint, &topic_arn, &queue_arn).await;

    // Create sink.
    let config = SnsSinkConfig {
        topic_arn: Some(topic_arn.clone()),
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint.clone()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
    };

    let sink = SnsSink::new(config).await.expect("Failed to create sink");

    // Publish events.
    let events = vec![make_test_event("event1"), make_test_event("event2")];
    sink.publish_events(events)
        .await
        .expect("Failed to publish");

    // Verify messages arrived via SQS.
    let messages = receive_sqs_messages(&endpoint, &queue_url, 2).await;
    assert_eq!(messages.len(), 2, "Expected 2 messages");

    // SNS wraps messages in an envelope, so we need to parse the outer JSON.
    // Only payload is sent now.
    for msg in &messages {
        let envelope: serde_json::Value =
            serde_json::from_str(msg).expect("Failed to parse envelope");
        let inner_msg = envelope["Message"].as_str().expect("Missing Message field");
        let payload: serde_json::Value =
            serde_json::from_str(inner_msg).expect("Failed to parse payload");
        // Payload fields from make_test_event
        assert!(payload["key"].is_string());
        assert!(payload["value"].is_string());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sns_sink_handles_empty_batch() {
    let port = ensure_localstack().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let topic_arn = create_sns_topic(&endpoint).await;

    let config = SnsSinkConfig {
        topic_arn: Some(topic_arn),
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
    };

    let sink = SnsSink::new(config).await.expect("Failed to create sink");

    // Publishing empty batch should succeed without making any API calls.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sns_sink_sends_only_payload() {
    let port = ensure_localstack().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let topic_arn = create_sns_topic(&endpoint).await;
    let queue_url = create_sqs_queue(&endpoint).await;
    let queue_arn = get_queue_arn(&endpoint, &queue_url).await;
    subscribe_sqs_to_sns(&endpoint, &topic_arn, &queue_arn).await;

    let config = SnsSinkConfig {
        topic_arn: Some(topic_arn),
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint.clone()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
    };

    let sink = SnsSink::new(config).await.expect("Failed to create sink");

    // Create event with metadata (metadata is used for routing, not sent).
    let event = TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "action": "created", "data": "test" }),
        metadata: Some(serde_json::json!({ "user_id": 123, "source": "api" })),
        lsn: Some(PgLsn::from(99999u64)),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    let messages = receive_sqs_messages(&endpoint, &queue_url, 1).await;
    assert_eq!(messages.len(), 1);

    let envelope: serde_json::Value =
        serde_json::from_str(&messages[0]).expect("Failed to parse envelope");
    let inner_msg = envelope["Message"].as_str().expect("Missing Message field");
    let payload: serde_json::Value =
        serde_json::from_str(inner_msg).expect("Failed to parse payload");

    // Only payload is sent - verify payload fields, not id/metadata/lsn.
    assert_eq!(payload["action"], "created");
    assert_eq!(payload["data"], "test");
    // These should NOT be in the message since we only send payload.
    assert!(payload.get("id").is_none());
    assert!(payload.get("metadata").is_none());
    assert!(payload.get("lsn").is_none());
}

#[test]
fn test_sink_name() {
    assert_eq!(SnsSink::name(), "sns");
}
