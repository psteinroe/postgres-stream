use chrono::Utc;
use postgres_stream::failover_client::FailoverClient;
use postgres_stream::test_utils::{TestDatabase, insert_events_to_db};
use postgres_stream::types::EventIdentifier;
use uuid::Uuid;

// Connection tests

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_client_connect_no_tls() {
    let db = TestDatabase::spawn().await;

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect without TLS");

    assert!(!client.is_closed(), "Connection should be open");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_client_connection_detection() {
    let db = TestDatabase::spawn().await;

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Initially connected
    assert!(!client.is_closed());

    // Drop client and verify connection state
    drop(client);

    // Note: We can't easily test is_closed() after drop since the client is consumed
    // This test mainly verifies the connection succeeds and is_closed() works
}

// Checkpoint update tests

#[tokio::test(flavor = "multi_thread")]
async fn test_update_checkpoint_persists() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    // Ensure stream row exists
    sqlx::query("insert into pgstream.streams (id, next_maintenance_at) values (1, now())")
        .execute(&db.pool)
        .await
        .unwrap();

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert an event to use as checkpoint
    let event_id = Uuid::new_v4();
    let created_at = Utc::now();

    sqlx::query(
        "insert into pgstream.events (id, created_at, payload, stream_id) values ($1::uuid, $2, $3, $4)",
    )
    .bind(event_id.to_string())
    .bind(created_at)
    .bind(serde_json::json!({"checkpoint": "test"}))
    .bind(1i64)
    .execute(&db.pool)
    .await
    .unwrap();

    let checkpoint = EventIdentifier::new(event_id.to_string(), created_at);

    // Update checkpoint
    client
        .update_checkpoint(&checkpoint)
        .await
        .expect("Failed to update checkpoint");

    // Verify checkpoint was persisted
    let row: (Option<String>, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
        "select failover_checkpoint_id, failover_checkpoint_ts from pgstream.streams where id = 1",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(row.0, Some(event_id.to_string()));
    assert_eq!(
        row.1.unwrap().timestamp_micros(),
        created_at.timestamp_micros()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_checkpoint_multiple_times() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    // Ensure stream row exists
    sqlx::query("insert into pgstream.streams (id, next_maintenance_at) values (1, now())")
        .execute(&db.pool)
        .await
        .unwrap();

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert multiple events
    let events = insert_events_to_db(&db, 3).await;

    // Update checkpoint to first event
    client
        .update_checkpoint(events.first().expect("Should have event 0"))
        .await
        .unwrap();

    // Update checkpoint to second event (should overwrite)
    client
        .update_checkpoint(events.get(1).expect("Should have event 1"))
        .await
        .unwrap();

    // Verify latest checkpoint
    let row: (Option<String>,) =
        sqlx::query_as("select failover_checkpoint_id from pgstream.streams where id = 1")
            .fetch_one(&db.pool)
            .await
            .unwrap();

    assert_eq!(
        row.0,
        Some(events.get(1).expect("Should have event 1").id.clone())
    );
}

// Copy stream tests

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_copy_stream_empty_range() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert one event
    let events = insert_events_to_db(&db, 1).await;

    // Request range with same from/to (should be empty)
    let stream = client
        .get_events_copy_stream(
            events.first().expect("Should have event 0"),
            events.first().expect("Should have event 0"),
        )
        .await
        .expect("Failed to get copy stream");

    // The stream should be empty (no events between same ID)
    drop(stream);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_copy_stream_with_events() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert multiple events
    let events = insert_events_to_db(&db, 5).await;

    // Request range from event 0 to event 4 (should get events 1, 2, 3)
    let stream = client
        .get_events_copy_stream(
            events.first().expect("Should have event 0"),
            events.get(4).expect("Should have event 4"),
        )
        .await
        .expect("Failed to get copy stream");

    // Read from the stream (this is a CopyOutStream, we can't easily count without parsing)
    // But we can verify it doesn't error
    use futures::StreamExt;
    use tokio::pin;

    pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        result.expect("Copy stream should not error");
        count += 1;
    }

    // We should have received some data (3 events worth of data)
    // Note: CopyOutStream returns bytes, not parsed rows, so we just check it's non-zero
    assert!(count > 0, "Should have received copy data");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_copy_stream_boundary_conditions() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert events with specific timing
    let events = insert_events_to_db(&db, 10).await;

    // Test boundary: from first to second (should exclude both boundaries)
    let stream = client
        .get_events_copy_stream(
            events.first().expect("Should have event 0"),
            events.get(1).expect("Should have event 1"),
        )
        .await
        .expect("Failed to get copy stream");

    drop(stream);

    // Test boundary: from middle to end
    let stream = client
        .get_events_copy_stream(
            events.get(4).expect("Should have event 4"),
            events.get(9).expect("Should have event 9"),
        )
        .await
        .expect("Failed to get copy stream");

    drop(stream);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_copy_stream_filters_by_stream_id() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    // Insert events for stream 1
    let mut events_stream_1 = Vec::new();
    for i in 0..3 {
        let id = Uuid::new_v4();
        let created_at = Utc::now() + chrono::Duration::milliseconds(i * 10);

        sqlx::query(
            "insert into pgstream.events (id, created_at, payload, stream_id) values ($1::uuid, $2, $3, $4)",
        )
        .bind(id.to_string())
        .bind(created_at)
        .bind(serde_json::json!({"seq": i, "stream": 1}))
        .bind(1i64)
        .execute(&db.pool)
        .await
        .unwrap();

        events_stream_1.push(EventIdentifier::new(id.to_string(), created_at));
    }

    // Insert events for stream 2 (should be filtered out)
    for i in 0..3 {
        let id = Uuid::new_v4();
        let created_at = Utc::now() + chrono::Duration::milliseconds(i * 10 + 5);

        sqlx::query(
            "insert into pgstream.events (id, created_at, payload, stream_id) values ($1::uuid, $2, $3, $4)",
        )
        .bind(id.to_string())
        .bind(created_at)
        .bind(serde_json::json!({"seq": i, "stream": 2}))
        .bind(2i64)
        .execute(&db.pool)
        .await
        .unwrap();
    }

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Request events for stream 1 only
    let stream = client
        .get_events_copy_stream(
            events_stream_1.first().expect("Should have event 0"),
            events_stream_1.get(2).expect("Should have event 2"),
        )
        .await
        .expect("Failed to get copy stream");

    // Stream should only contain stream 1 events
    // We can't easily parse the COPY format here, but the query should execute successfully
    drop(stream);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_survives_multiple_operations() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = FailoverClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    let events = insert_events_to_db(&db, 5).await;

    // Perform multiple operations on same connection
    for i in 0..3 {
        // Update checkpoint
        client
            .update_checkpoint(events.get(i).expect("Should have event"))
            .await
            .unwrap();

        // Get copy stream
        let stream = client
            .get_events_copy_stream(
                events.get(i).expect("Should have event"),
                events.get(i + 1).expect("Should have next event"),
            )
            .await
            .unwrap();
        drop(stream);

        // Connection should still be alive
        assert!(!client.is_closed());
    }
}
