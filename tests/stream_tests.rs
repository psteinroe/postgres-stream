use etl::destination::Destination;
use etl::store::both::postgres::PostgresStore;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::store::StreamStore;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    FailableSink, TestDatabase, create_postgres_store, create_postgres_store_with_table_id,
    insert_events_to_db, make_event_with_id, make_test_event, test_stream_config,
};
use postgres_stream::types::{StreamStatus, TriggeredEvent};

// Basic stream tests

#[tokio::test(flavor = "multi_thread")]
async fn test_pgstream_create() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let _stream: PgStream<MemorySink, PostgresStore> = PgStream::create(config, sink, store)
        .await
        .expect("Failed to create PgStream");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pgstream_write_events_via_destination_trait() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> = PgStream::create(config, sink.clone(), store)
        .await
        .expect("Failed to create PgStream");

    let events = vec![
        make_test_event(table_id, serde_json::json!({"id": 1, "name": "Alice"})),
        make_test_event(table_id, serde_json::json!({"id": 2, "name": "Bob"})),
    ];

    stream
        .write_events(events)
        .await
        .expect("Failed to write events");

    let stored_events = sink.events().await;
    assert_eq!(stored_events.len(), 2);
}

// Failover tests

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_sink_failure_enters_failover_mode() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 1).await;

    sink.fail_on_call(0);

    let first_event_id = event_ids.first().expect("Should have inserted 1 event");
    let events = vec![make_event_with_id(
        table_id,
        first_event_id,
        serde_json::json!({"id": 1}),
    )];
    stream
        .write_events(events)
        .await
        .expect("write_events should succeed even if sink fails");

    let stream_store = StreamStore::create(config, store).await.unwrap();
    let (status, _) = stream_store.get_stream_state().await.unwrap();

    match status {
        StreamStatus::Failover {
            checkpoint_event_id,
        } => {
            assert_eq!(checkpoint_event_id.id, first_event_id.id);
            // Compare timestamps at microsecond precision (PostgreSQL precision)
            assert_eq!(
                checkpoint_event_id.created_at.timestamp_micros(),
                first_event_id.created_at.timestamp_micros()
            );
        }
        StreamStatus::Healthy => panic!("Expected Failover status, got Healthy"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_recovery_replays_missed_events() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream");

    let all_event_ids = insert_events_to_db(&db, 6).await;

    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            all_event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )])
        .await
        .unwrap();

    sink.fail_on_call(1);

    stream
        .write_events(vec![make_event_with_id(
            table_id,
            all_event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )])
        .await
        .unwrap();

    assert_eq!(sink.call_count(), 2);

    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            all_event_ids.get(5).expect("Should have event 5"),
            serde_json::json!({"seq": 6}),
        )])
        .await
        .unwrap();

    let published: Vec<TriggeredEvent> = sink.events().await;

    assert!(
        published.len() >= 6,
        "Expected at least 6 events, got {}",
        published.len()
    );

    let stream_store = StreamStore::create(config, store).await.unwrap();
    let (status, _) = stream_store.get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should return to Healthy after recovery"
    );
}

// Extended failover scenarios

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_with_no_missed_events() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 3).await;

    // First event succeeds
    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )])
        .await
        .unwrap();

    // Second event fails, entering failover
    sink.fail_on_call(1);
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )])
        .await
        .unwrap();

    // Third event succeeds - should replay the failed event and then process the new one
    // Since event 1 failed and event 2 is the next, there are no "missed" events between them
    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )])
        .await
        .unwrap();

    let published = sink.events().await;

    // Should have published: event 0 (success), event 1 (retry), event 2 (new)
    assert!(
        published.len() >= 3,
        "Expected at least 3 events, got {}",
        published.len()
    );

    let stream_store = StreamStore::create(config, store).await.unwrap();
    let (status, _) = stream_store.get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should return to Healthy after recovery"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_multiple_consecutive_failures() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 5).await;

    // Event 0: success
    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )])
        .await
        .unwrap();

    // Event 1: fail - enters failover
    sink.fail_on_call(1);
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )])
        .await
        .unwrap();

    // Event 2: fail again - should retry event 1 first, then fail on event 2
    sink.fail_on_call(2);
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )])
        .await
        .unwrap();

    // Event 4: succeed - should replay events 1, 2, 3 and then process 4
    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(4).expect("Should have event 4"),
            serde_json::json!({"seq": 5}),
        )])
        .await
        .unwrap();

    let published = sink.events().await;

    // Should eventually publish all events
    assert!(
        published.len() >= 5,
        "Expected at least 5 events, got {}",
        published.len()
    );

    let stream_store = StreamStore::create(config, store).await.unwrap();
    let (status, _) = stream_store.get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should recover to Healthy even after multiple failures"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_large_gap_recovery() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream");

    // Insert a larger number of events to test recovery with bigger gaps
    let event_ids = insert_events_to_db(&db, 50).await;

    // Event 0: success
    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )])
        .await
        .unwrap();

    // Event 5: fail - creates gap of events 1-4
    sink.fail_on_call(1);
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(5).expect("Should have event 5"),
            serde_json::json!({"seq": 6}),
        )])
        .await
        .unwrap();

    // Event 49: succeed - should replay all missed events (5-48) plus new event
    sink.succeed_always();
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(49).expect("Should have event 49"),
            serde_json::json!({"seq": 50}),
        )])
        .await
        .unwrap();

    let published = sink.events().await;

    // Should have: event 0 (success) + events 5-48 (replayed) + event 49 (new) = 46 events
    assert!(
        published.len() >= 46,
        "Expected at least 46 events after gap recovery, got {}",
        published.len()
    );

    let stream_store = StreamStore::create(config, store).await.unwrap();
    let (status, _) = stream_store.get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should recover to Healthy after large gap"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_checkpoint_persists_across_stream_instances() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    // Create first stream instance
    let stream1: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 3).await;

    // Event 0: success
    sink.succeed_always();
    stream1
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )])
        .await
        .unwrap();

    // Event 1: fail - enters failover
    sink.fail_on_call(1);
    stream1
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )])
        .await
        .unwrap();

    // Drop the first stream (simulating restart)
    drop(stream1);

    // Create a new stream instance - should pick up failover state
    let stream2: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .expect("Failed to create PgStream after restart");

    // Verify it's in failover state
    let stream_store = StreamStore::create(config.clone(), store.clone())
        .await
        .unwrap();
    let (status, _) = stream_store.get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Failover { .. }),
        "Stream should be in Failover state after restart"
    );

    // Event 2: succeed - should recover
    sink.succeed_always();
    stream2
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )])
        .await
        .unwrap();

    let published = sink.events().await;

    // Should have recovered and published all events
    assert!(
        published.len() >= 3,
        "Expected at least 3 events after recovery, got {}",
        published.len()
    );

    let stream_store2 = StreamStore::create(config, store).await.unwrap();
    let (status, _) = stream_store2.get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should be Healthy after recovery"
    );
}
