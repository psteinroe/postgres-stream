#![allow(clippy::indexing_slicing)]
//! Integration tests for AWS Kinesis sink.
//!
//! Uses LocalStack container to test Kinesis record publishing.

#![cfg(feature = "sink-kinesis")]

use chrono::Utc;
use postgres_stream::sink::Sink;
use postgres_stream::sink::kinesis::{KinesisSink, KinesisSinkConfig};
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

/// Creates a Kinesis stream in LocalStack.
async fn create_kinesis_stream(endpoint: &str, stream_name: &str) {
    let region = aws_sdk_kinesis::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_kinesis::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_kinesis::Client::new(&sdk_config);

    client
        .create_stream()
        .stream_name(stream_name)
        .shard_count(1)
        .send()
        .await
        .expect("Failed to create Kinesis stream");

    // Wait for stream to become active.
    for _ in 0..30 {
        let desc = client
            .describe_stream()
            .stream_name(stream_name)
            .send()
            .await
            .expect("Failed to describe stream");

        if let Some(stream_desc) = desc.stream_description()
            && stream_desc.stream_status() == &aws_sdk_kinesis::types::StreamStatus::Active
        {
            return;
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    panic!("Stream did not become active in time");
}

/// Gets records from a Kinesis stream.
async fn get_kinesis_records(endpoint: &str, stream_name: &str, count: usize) -> Vec<String> {
    let region = aws_sdk_kinesis::config::Region::new("us-east-1");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_kinesis::config::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;

    let client = aws_sdk_kinesis::Client::new(&sdk_config);

    // Get shard iterator.
    let shards = client
        .list_shards()
        .stream_name(stream_name)
        .send()
        .await
        .expect("Failed to list shards");

    let shard_id = shards.shards().first().expect("No shards found").shard_id();

    let iterator_response = client
        .get_shard_iterator()
        .stream_name(stream_name)
        .shard_id(shard_id)
        .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .expect("Failed to get shard iterator");

    let mut shard_iterator = iterator_response.shard_iterator().unwrap().to_string();
    let mut records = Vec::new();

    // Poll for records.
    for _ in 0..10 {
        let response = client
            .get_records()
            .shard_iterator(&shard_iterator)
            .limit(100)
            .send()
            .await
            .expect("Failed to get records");

        for record in response.records() {
            let data = String::from_utf8(record.data().as_ref().to_vec())
                .expect("Invalid UTF-8 in record");
            records.push(data);
        }

        if records.len() >= count {
            break;
        }

        if let Some(next_iterator) = response.next_shard_iterator() {
            shard_iterator = next_iterator.to_string();
        } else {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    records
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kinesis_sink_publishes_events() {
    let port = ensure_localstack().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let stream_name = format!("test-stream-{}", Uuid::new_v4());
    create_kinesis_stream(&endpoint, &stream_name).await;

    // Create sink.
    let config = KinesisSinkConfig {
        stream_name: Some(stream_name.clone()),
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint.clone()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
    };

    let sink = KinesisSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publish events.
    let events = vec![make_test_event("event1"), make_test_event("event2")];
    sink.publish_events(events)
        .await
        .expect("Failed to publish");

    // Verify records arrived - only payload is sent.
    let records = get_kinesis_records(&endpoint, &stream_name, 2).await;
    assert_eq!(records.len(), 2, "Expected 2 records");

    for record in &records {
        let payload: serde_json::Value =
            serde_json::from_str(record).expect("Failed to parse record");
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
async fn test_kinesis_sink_handles_empty_batch() {
    let port = ensure_localstack().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let stream_name = format!("test-stream-{}", Uuid::new_v4());
    create_kinesis_stream(&endpoint, &stream_name).await;

    let config = KinesisSinkConfig {
        stream_name: Some(stream_name),
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
    };

    let sink = KinesisSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publishing empty batch should succeed without making any API calls.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_kinesis_sink_uses_stream_from_metadata() {
    let port = ensure_localstack().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let stream_name = format!("test-stream-{}", Uuid::new_v4());
    create_kinesis_stream(&endpoint, &stream_name).await;

    // Create sink without stream_name - will get it from metadata.
    let config = KinesisSinkConfig {
        stream_name: None,
        region: "us-east-1".to_string(),
        endpoint_url: Some(endpoint.clone()),
        access_key_id: Some("test".to_string()),
        secret_access_key: Some("test".to_string()),
    };

    let sink = KinesisSink::new(config)
        .await
        .expect("Failed to create sink");

    // Create event with stream in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "action": "created" }),
        metadata: Some(serde_json::json!({ "stream": stream_name })),
        lsn: Some(PgLsn::from(99999u64)),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    let records = get_kinesis_records(&endpoint, &stream_name, 1).await;
    assert_eq!(records.len(), 1);

    let payload: serde_json::Value =
        serde_json::from_str(&records[0]).expect("Failed to parse record");

    // Only payload is sent.
    assert_eq!(payload["action"], "created");
    assert!(payload.get("id").is_none());
    assert!(payload.get("metadata").is_none());
}

#[test]
fn test_sink_name() {
    assert_eq!(KinesisSink::name(), "kinesis");
}
