#![allow(clippy::indexing_slicing)]
//! Integration tests for Elasticsearch sink.
//!
//! Uses Elasticsearch testcontainer to test document indexing.

#![cfg(feature = "sink-elasticsearch")]

use chrono::Utc;
use elasticsearch::http::transport::Transport;
use elasticsearch::indices::IndicesRefreshParts;
use elasticsearch::{Elasticsearch, GetParts, SearchParts};
use postgres_stream::sink::Sink;
use postgres_stream::sink::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use postgres_stream::test_utils::ensure_elasticsearch;
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

/// Creates an Elasticsearch client for testing.
async fn create_test_client(port: u16) -> Elasticsearch {
    let url = format!("http://127.0.0.1:{port}");
    let transport = Transport::single_node(&url).expect("Failed to create transport");
    Elasticsearch::new(transport)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_elasticsearch_sink_indexes_events() {
    let port = ensure_elasticsearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    // Create sink.
    let config = ElasticsearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name.clone()),
    };

    let sink = ElasticsearchSink::new(config)
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

    // Refresh index to make documents searchable.
    let client = create_test_client(port).await;
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&index_name]))
        .send()
        .await
        .expect("Failed to refresh index");

    // Verify documents exist.
    let response = client
        .get(GetParts::IndexId(&index_name, &event1_id))
        .send()
        .await
        .expect("Failed to get document");

    assert!(
        response.status_code().is_success(),
        "Document not found: {event1_id}"
    );

    let response = client
        .get(GetParts::IndexId(&index_name, &event2_id))
        .send()
        .await
        .expect("Failed to get document");

    assert!(
        response.status_code().is_success(),
        "Document not found: {event2_id}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_elasticsearch_sink_handles_empty_batch() {
    let port = ensure_elasticsearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    let config = ElasticsearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name),
    };

    let sink = ElasticsearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Publishing empty batch should succeed without making any API calls.
    sink.publish_events(vec![])
        .await
        .expect("Empty batch should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_elasticsearch_sink_indexes_only_payload() {
    let port = ensure_elasticsearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    let config = ElasticsearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name.clone()),
    };

    let sink = ElasticsearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Create event with metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "action": "created", "user_id": 456 }),
        metadata: Some(serde_json::json!({ "source": "api" })),
        commit_lsn: None,
        lsn: Some(PgLsn::from(99999u64)),
    };
    let event_id = event.id.id.clone();

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    // Refresh and retrieve document.
    let client = create_test_client(port).await;
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&index_name]))
        .send()
        .await
        .expect("Failed to refresh");

    let response = client
        .get(GetParts::IndexId(&index_name, &event_id))
        .send()
        .await
        .expect("Failed to get document");

    let body = response
        .json::<serde_json::Value>()
        .await
        .expect("Failed to parse response");

    let source = &body["_source"];
    // Only payload fields should be present.
    assert_eq!(source["action"], "created");
    assert_eq!(source["user_id"], 456);
    // No envelope fields.
    assert!(source.get("id").is_none());
    assert!(source.get("created_at").is_none());
    assert!(source.get("metadata").is_none());
    assert!(source.get("lsn").is_none());
    assert!(source.get("stream_id").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_elasticsearch_sink_searchable() {
    let port = ensure_elasticsearch().await;
    let index_name = format!("test-index-{}", Uuid::new_v4());

    let config = ElasticsearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: Some(index_name.clone()),
    };

    let sink = ElasticsearchSink::new(config)
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

    // Refresh index.
    let client = create_test_client(port).await;
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&index_name]))
        .send()
        .await
        .expect("Failed to refresh");

    // Search for documents.
    let response = client
        .search(SearchParts::Index(&[&index_name]))
        .body(serde_json::json!({
            "query": { "match_all": {} }
        }))
        .send()
        .await
        .expect("Failed to search");

    let body = response
        .json::<serde_json::Value>()
        .await
        .expect("Failed to parse response");

    let hits = body["hits"]["total"]["value"]
        .as_i64()
        .expect("Missing hit count");
    assert_eq!(hits, 3, "Expected 3 documents");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_elasticsearch_sink_uses_index_from_metadata() {
    let port = ensure_elasticsearch().await;
    let metadata_index = format!("metadata-index-{}", Uuid::new_v4());

    // Create sink with NO default index.
    let config = ElasticsearchSinkConfig {
        url: format!("http://127.0.0.1:{port}"),
        index: None,
    };

    let sink = ElasticsearchSink::new(config)
        .await
        .expect("Failed to create sink");

    // Create event with index in metadata.
    let event = TriggeredEvent {
        id: EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now()),
        stream_id: StreamId::default(),
        payload: serde_json::json!({ "routed": true }),
        metadata: Some(serde_json::json!({ "index": metadata_index })),
        commit_lsn: None,
        lsn: None,
    };
    let event_id = event.id.id.clone();

    sink.publish_events(vec![event])
        .await
        .expect("Failed to publish");

    // Verify document was indexed to metadata-specified index.
    let client = create_test_client(port).await;
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&metadata_index]))
        .send()
        .await
        .expect("Failed to refresh");

    let response = client
        .get(GetParts::IndexId(&metadata_index, &event_id))
        .send()
        .await
        .expect("Failed to get document");

    assert!(
        response.status_code().is_success(),
        "Document not found in metadata-specified index"
    );

    let body = response
        .json::<serde_json::Value>()
        .await
        .expect("Failed to parse response");

    assert_eq!(body["_source"]["routed"], true);
}

#[test]
fn test_sink_name() {
    assert_eq!(ElasticsearchSink::name(), "elasticsearch");
}
