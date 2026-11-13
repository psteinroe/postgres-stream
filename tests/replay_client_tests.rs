use chrono::Utc;
use postgres_stream::replay_client::ReplayClient;
use postgres_stream::test_utils::{TestDatabase, insert_events_to_db};
use postgres_stream::types::EventIdentifier;
use uuid::Uuid;

// Connection tests

#[tokio::test(flavor = "multi_thread")]
async fn test_replay_client_connect_no_tls() {
    let db = TestDatabase::spawn().await;

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect without TLS");

    assert!(!client.is_closed(), "Connection should be open");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replay_client_connection_detection() {
    let db = TestDatabase::spawn().await;

    let client = ReplayClient::connect(1, db.config.clone())
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

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert an event to use as checkpoint
    let event_id = Uuid::new_v4();
    // Use today's date with a fixed time to ensure deterministic ordering in tests
    let today = Utc::now().date_naive();
    let created_at = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();

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

    let client = ReplayClient::connect(1, db.config.clone())
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

// Events stream tests

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_stream_empty_range() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert one event
    let events = insert_events_to_db(&db, 1).await;

    // Request range with same from/to (should be empty)
    let stream = client
        .get_events_stream(
            events.first().expect("Should have event 0"),
            events.first().expect("Should have event 0"),
        )
        .await
        .expect("Failed to get events stream");

    // The stream should be empty (no events between same ID)
    use futures::StreamExt;
    use tokio::pin;

    pin!(stream);

    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
    }

    assert_eq!(count, 0, "Stream should be empty for same from/to range");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_stream_with_events() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert multiple events with a trigger-time row LSN.
    let events = insert_events_to_db(&db, 5).await;
    sqlx::query("UPDATE pgstream.events SET lsn = '0/100'::pg_lsn WHERE stream_id = 1")
        .execute(&db.pool)
        .await
        .unwrap();

    // Request range from event 0 to event 4 (should get events 1, 2, 3)
    let stream = client
        .get_events_stream(
            events.first().expect("Should have event 0"),
            events.get(4).expect("Should have event 4"),
        )
        .await
        .expect("Failed to get events stream");

    use futures::StreamExt;
    use tokio::pin;
    pin!(stream);

    let mut replayed_events = Vec::new();
    while let Some(result) = stream.next().await {
        replayed_events.push(result.expect("Should parse row successfully"));
    }

    // Should get events 1, 2, 3 (events between 0 and 4, exclusive)
    assert_eq!(
        replayed_events.len(),
        3,
        "Should have received exactly 3 events"
    );
    let expected_ids = events.get(1..4).unwrap().to_vec();
    let replayed_ids: Vec<_> = replayed_events
        .iter()
        .map(|event| event.id.clone())
        .collect();
    assert_eq!(replayed_ids, expected_ids);
    assert!(replayed_events.iter().all(|event| {
        event.lsn == Some("0/100".parse().unwrap()) && event.commit_lsn.is_none()
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_stream_boundary_conditions() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert events with specific timing
    let events = insert_events_to_db(&db, 10).await;
    use futures::StreamExt;
    use tokio::pin;

    // Test boundary: from first to second (should exclude both boundaries, so 0 events)
    let stream = client
        .get_events_stream(
            events.first().expect("Should have event 0"),
            events.get(1).expect("Should have event 1"),
        )
        .await
        .expect("Failed to get events stream");
    pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let _row = result.expect("Should parse row successfully");
        count += 1;
    }
    assert_eq!(count, 0, "Should have 0 events between consecutive IDs");

    // Test boundary: from middle to end (events 5, 6, 7, 8)
    let stream = client
        .get_events_stream(
            events.get(4).expect("Should have event 4"),
            events.get(9).expect("Should have event 9"),
        )
        .await
        .expect("Failed to get events stream");
    pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let _row = result.expect("Should parse row successfully");
        count += 1;
    }
    assert_eq!(count, 4, "Should have 4 events between indices 4 and 9");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_checkpoint_while_events_stream_active() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    // Ensure stream row exists for checkpoint updates.
    sqlx::query("insert into pgstream.streams (id, next_maintenance_at) values (1, now())")
        .execute(&db.pool)
        .await
        .unwrap();

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Insert enough events so COPY remains active after reading first batch.
    let events = insert_events_to_db(&db, 250).await;

    // Keep COPY active by only partially consuming it.
    use futures::StreamExt;
    use tokio::pin;

    let stream = client
        .get_events_stream(
            events.first().expect("Should have event 0"),
            events.get(249).expect("Should have event 249"),
        )
        .await
        .expect("Failed to get events stream");
    pin!(stream);

    for _ in 0..100 {
        let row = stream
            .next()
            .await
            .expect("Expected row from copy stream")
            .expect("Should parse row successfully");
        drop(row);
    }

    // Checkpoint update should not block while COPY stream is still active.
    let checkpoint_update = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        client.update_checkpoint(events.get(100).expect("Should have event 100")),
    )
    .await;

    assert!(
        checkpoint_update.is_ok(),
        "Checkpoint update should not time out while copy stream is active"
    );
    checkpoint_update.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_events_stream_filters_by_stream_id() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    // Insert events for stream 1
    let mut events_stream_1 = Vec::new();
    // Use today's date with a fixed time to ensure deterministic ordering in tests
    let today = Utc::now().date_naive();
    let base_time = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();

    for i in 0..3 {
        let id = Uuid::new_v4();
        // Use seconds for spacing to ensure distinct timestamps
        let created_at = base_time + chrono::Duration::seconds(i * 2);

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

    // Insert events for stream 2 (should be filtered out) - interleave with stream 1
    for i in 0..3 {
        let id = Uuid::new_v4();
        // Offset by 1 second so they interleave: stream1@0s, stream2@1s, stream1@2s, stream2@3s, etc
        let created_at = base_time + chrono::Duration::seconds(i * 2 + 1);

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

    let client = ReplayClient::connect(1, db.config.clone())
        .await
        .expect("Failed to connect");

    // Request events for stream 1 only (from event 0 to event 2, should get event 1)
    let stream = client
        .get_events_stream(
            events_stream_1.first().expect("Should have event 0"),
            events_stream_1.get(2).expect("Should have event 2"),
        )
        .await
        .expect("Failed to get events stream");

    // Wrap with TableCopyStream to parse and verify only stream 1 events are returned
    use futures::StreamExt;
    use tokio::pin;
    pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let _row = result.expect("Should parse row successfully");
        count += 1;
    }

    // Should only get event 1 from stream 1 (stream 2 events should be filtered out)
    assert_eq!(count, 1, "Should have exactly 1 event from stream 1");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_survives_multiple_operations() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    let client = ReplayClient::connect(1, db.config.clone())
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
            .get_events_stream(
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
