//! Integration tests for the Webhook sink.

#![cfg(feature = "sink-webhook")]

use postgres_stream::sink::Sink;
use postgres_stream::sink::webhook::{WebhookSink, WebhookSinkConfig};
use postgres_stream::types::{EventIdentifier, StreamId, TriggeredEvent};

use chrono::Utc;
use std::collections::HashMap;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
async fn test_webhook_sink_publishes_events() {
    let mock_server = MockServer::start().await;

    // Expect one POST request with array of payloads (events are batched by URL).
    Mock::given(method("POST"))
        .and(path("/events"))
        .and(header("Content-Type", "application/json"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = WebhookSinkConfig {
        url: Some(format!("{}/events", mock_server.uri())),
        headers: HashMap::new(),
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    // Publish test events.
    let events = vec![
        make_test_event("webhook-event-1"),
        make_test_event("webhook-event-2"),
    ];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    // Verify mock expectations were met.
    mock_server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webhook_sink_with_custom_headers() {
    let mock_server = MockServer::start().await;

    // Expect request with custom header.
    Mock::given(method("POST"))
        .and(path("/events"))
        .and(header("X-Custom-Header", "custom-value"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut headers = HashMap::new();
    headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());
    headers.insert("Authorization".to_string(), "Bearer test-token".to_string());

    let config = WebhookSinkConfig {
        url: Some(format!("{}/events", mock_server.uri())),
        headers,
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    let events = vec![make_test_event("header-test-event")];
    sink.publish_events(events)
        .await
        .expect("Failed to publish events");

    mock_server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webhook_sink_handles_server_error() {
    let mock_server = MockServer::start().await;

    // Return 500 error.
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let config = WebhookSinkConfig {
        url: Some(format!("{}/events", mock_server.uri())),
        headers: HashMap::new(),
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    let events = vec![make_test_event("error-test-event")];
    let result = sink.publish_events(events).await;

    // Should fail with error status.
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("500"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webhook_sink_empty_batch() {
    let mock_server = MockServer::start().await;

    // Should NOT receive any requests for empty batch.
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock_server)
        .await;

    let config = WebhookSinkConfig {
        url: Some(format!("{}/events", mock_server.uri())),
        headers: HashMap::new(),
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    // Empty batch should succeed without making requests.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");

    mock_server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webhook_sink_url_from_metadata() {
    let mock_server = MockServer::start().await;

    // Expect request to the dynamic URL from metadata.
    Mock::given(method("POST"))
        .and(path("/dynamic-endpoint"))
        .and(header("Content-Type", "application/json"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Config with no URL - it must come from metadata.
    let config = WebhookSinkConfig {
        url: None,
        headers: HashMap::new(),
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    // Create event with URL in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("metadata-url-event".to_string(), Utc::now()),
        payload: serde_json::json!({ "test": "data" }),
        metadata: Some(serde_json::json!({
            "url": format!("{}/dynamic-endpoint", mock_server.uri())
        })),
        stream_id: StreamId::from(1u64),
        lsn: None,
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish event with metadata URL");

    mock_server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webhook_sink_headers_from_metadata() {
    let mock_server = MockServer::start().await;

    // Expect request with headers from both config and metadata.
    Mock::given(method("POST"))
        .and(path("/events"))
        .and(header("X-Config-Header", "from-config"))
        .and(header("X-Metadata-Header", "from-metadata"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let mut config_headers = HashMap::new();
    config_headers.insert("X-Config-Header".to_string(), "from-config".to_string());

    let config = WebhookSinkConfig {
        url: Some(format!("{}/events", mock_server.uri())),
        headers: config_headers,
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    // Create event with additional headers in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("metadata-headers-event".to_string(), Utc::now()),
        payload: serde_json::json!({ "test": "data" }),
        metadata: Some(serde_json::json!({
            "headers": {
                "X-Metadata-Header": "from-metadata"
            }
        })),
        stream_id: StreamId::from(1u64),
        lsn: None,
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish event with metadata headers");

    mock_server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webhook_sink_no_url_configured_fails() {
    // Config with no URL.
    let config = WebhookSinkConfig {
        url: None,
        headers: HashMap::new(),
        timeout_ms: 5000,
    };

    let sink = WebhookSink::new(config).expect("Failed to create Webhook sink");

    // Event without URL in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new("no-url-event".to_string(), Utc::now()),
        payload: serde_json::json!({ "test": "data" }),
        metadata: None,
        stream_id: StreamId::from(1u64),
        lsn: None,
    };

    let result = sink.publish_events(vec![event]).await;

    // Should fail because no URL is configured.
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("No URL configured")
    );
}

#[test]
fn test_sink_name() {
    assert_eq!(WebhookSink::name(), "webhook");
}
