//! Integration tests for Meilisearch sink.
//!
//! Uses Meilisearch testcontainer to test document indexing.

#![cfg(feature = "sink-meilisearch")]

use chrono::Utc;
use meilisearch_sdk::client::Client;
use postgres_stream::sink::Sink;
use postgres_stream::sink::meilisearch::{MeilisearchSink, MeilisearchSinkConfig};
use postgres_stream::test_utils::ensure_meilisearch;
use postgres_stream::types::{EventIdentifier, PgLsn, StreamId, TriggeredEvent};
use uuid::Uuid;

/// Creates a test event with the given payload key.
/// Includes an `id` field in payload (as users would do via payload_extensions).
fn make_test_event(key: &str) -> TriggeredEvent {
    let id = Uuid::new_v4().to_string();
    TriggeredEvent {
        id: EventIdentifier::new(id.clone(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "id": id, "key": key, "value": "test_data" }),
        metadata: Some(serde_json::json!({ "source": "test" })),
        lsn: Some(PgLsn::from(12345u64)),
    }
}

/// Creates a Meilisearch client for testing.
fn create_test_client(port: u16) -> Client {
    let url = format!("http://127.0.0.1:{port}");
    Client::new(&url, Option::<&str>::None).expect("Failed to create client")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meilisearch_sink_indexes_events() {
    let port = ensure_meilisearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    // Create sink.
    let config = MeilisearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name.clone()),
        api_key: None,
    };

    let sink = MeilisearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publish events.
    let event1 = make_test_event("event1");
    let event2 = make_test_event("event2");
    let event1_id = event1.id.id.clone();
    let event2_id = event2.id.id.clone();

    sink.publish_events(vec![event1, event2])
        .await
        .expect("Failed to publish");

    // Verify documents exist.
    let client = create_test_client(port);
    let index = client.index(&index_name);

    let doc1: serde_json::Value = index
        .get_document(&event1_id)
        .await
        .expect("Failed to get document");
    assert_eq!(doc1["id"], event1_id);

    let doc2: serde_json::Value = index
        .get_document(&event2_id)
        .await
        .expect("Failed to get document");
    assert_eq!(doc2["id"], event2_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meilisearch_sink_handles_empty_batch() {
    let port = ensure_meilisearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    let config = MeilisearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name),
        api_key: None,
    };

    let sink = MeilisearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publishing empty batch should succeed without making any API calls.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meilisearch_sink_indexes_only_payload() {
    let port = ensure_meilisearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    let config = MeilisearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name.clone()),
        api_key: None,
    };

    let sink = MeilisearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Create event with metadata.
    // Include `id` in payload (as users would do via payload_extensions).
    let event_id = Uuid::new_v4().to_string();
    let event = TriggeredEvent {
        id: EventIdentifier::new(event_id.clone(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "id": event_id, "action": "created", "user_id": 456 }),
        metadata: Some(serde_json::json!({ "source": "api" })),
        lsn: Some(PgLsn::from(99999u64)),
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    // Retrieve document.
    let client = create_test_client(port);
    let index = client.index(&index_name);

    let doc: serde_json::Value = index
        .get_document(&event_id)
        .await
        .expect("Failed to get document");

    // Only payload fields should be present (id from payload, not injected).
    assert_eq!(doc["action"], "created");
    assert_eq!(doc["user_id"], 456);
    assert_eq!(doc["id"], event_id); // From payload, not injected.
    // No envelope fields.
    assert!(doc.get("created_at").is_none());
    assert!(doc.get("metadata").is_none());
    assert!(doc.get("lsn").is_none());
    assert!(doc.get("stream_id").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meilisearch_sink_searchable() {
    let port = ensure_meilisearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    let config = MeilisearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name.clone()),
        api_key: None,
    };

    let sink = MeilisearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publish events with different keys.
    let events = vec![
        make_test_event("alpha"),
        make_test_event("beta"),
        make_test_event("gamma"),
    ];

    sink.publish_events(events)
        .await
        .expect("Failed to publish");

    // Search for documents.
    let client = create_test_client(port);
    let index = client.index(&index_name);

    // Wait for indexing to complete.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let results = index
        .search()
        .execute::<serde_json::Value>()
        .await
        .expect("Failed to search");

    assert_eq!(
        results.hits.len(),
        3,
        "Expected 3 documents, got {}",
        results.hits.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meilisearch_sink_uses_index_from_metadata() {
    let port = ensure_meilisearch().await;
    let metadata_index = format!("metadata-index-{}", Uuid::new_v4());

    // Create sink with NO default index.
    let config = MeilisearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: None,
        api_key: None,
    };

    let sink = MeilisearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Create event with index in metadata.
    // Include `id` in payload (as users would do via payload_extensions).
    let event_id = Uuid::new_v4().to_string();
    let event = TriggeredEvent {
        id: EventIdentifier::new(event_id.clone(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "id": event_id, "routed": true }),
        metadata: Some(serde_json::json!({ "index": metadata_index })),
        lsn: None,
    };

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    // Verify document was indexed to metadata-specified index.
    let client = create_test_client(port);
    let index = client.index(&metadata_index);

    let doc: serde_json::Value = index
        .get_document(&event_id)
        .await
        .expect("Failed to get document from metadata-specified index");

    assert_eq!(doc["routed"], true);
    assert_eq!(doc["id"], event_id);
}

#[test]
fn test_sink_name() {
    assert_eq!(MeilisearchSink::name(), "meilisearch");
}
