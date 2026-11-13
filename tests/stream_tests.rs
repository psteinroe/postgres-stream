use chrono::Duration;
use etl::data::Cell;
use etl::destination::{Destination, DestinationWriteStatus};
use etl::event::Event;
use etl::schema::PgLsn;
use etl::store::PostgresStore;
use postgres_stream::config::StreamConfig;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    FailableSink, TestDatabase, create_postgres_store, create_postgres_store_with_table_id,
    insert_events_to_db, make_event_with_id, make_test_event, test_stream_config, write_events,
};
use postgres_stream::types::{EventIdentifier, StreamStatus, TriggeredEvent};
use std::collections::HashSet;
use std::time::Duration as StdDuration;

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

    let row_lsn: PgLsn = "0/100".parse().unwrap();
    let commit_lsn: PgLsn = "0/200".parse().unwrap();
    let events = [
        serde_json::json!({"id": 1, "name": "Alice"}),
        serde_json::json!({"id": 2, "name": "Bob"}),
    ]
    .into_iter()
    .map(|payload| {
        let mut event = make_test_event(table_id, payload);
        let Event::Insert(insert_event) = &mut event else {
            panic!("test event should be an insert");
        };
        insert_event.commit_lsn = commit_lsn;
        insert_event
            .table_row
            .values_mut()
            .push(Cell::String("0/100".to_string()));
        event
    })
    .collect();

    write_events(&stream, events)
        .await
        .expect("Failed to write events");

    let stored_events = sink.events().await;
    assert_eq!(stored_events.len(), 2);
    assert!(
        stored_events
            .iter()
            .all(|event| event.lsn == Some(row_lsn) && event.commit_lsn == Some(commit_lsn))
    );
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
        PgStream::create(config.clone(), sink.clone(), store)
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
    write_events(&stream, events)
        .await
        .expect("write_events should succeed even if sink fails");

    let (status, _) = stream.store().get_stream_state().await.unwrap();

    match status {
        StreamStatus::Failover {
            checkpoint_event_id,
        } => {
            assert_eq!(checkpoint_event_id.id, first_event_id.id);
            // Compare timestamps at microsecond precision (Postgres precision)
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
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let all_event_ids = insert_events_to_db(&db, 6).await;

    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            all_event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    sink.fail_on_call(1);

    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            all_event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    assert_eq!(sink.call_count(), 2);

    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            all_event_ids.get(5).expect("Should have event 5"),
            serde_json::json!({"seq": 6}),
        )],
    )
    .await
    .unwrap();

    let published: Vec<TriggeredEvent> = sink.events().await;

    // Should have all 6 events: event 0 (initial), events 1-4 (replayed), event 5 (new)
    assert_eq!(published.len(), 6, "Should have exactly 6 events published");

    // Verify all event IDs are present
    let published_ids: Vec<String> = published.iter().map(|e| e.id.id.clone()).collect();
    for event_id in &all_event_ids {
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }

    let (status, _) = stream.store().get_stream_state().await.unwrap();
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
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 3).await;

    // First event succeeds
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Second event fails, entering failover
    sink.fail_on_call(1);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    // Third event succeeds - should replay the failed event and then process the new one
    // Since event 1 failed and event 2 is the next, there are no "missed" events between them
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )],
    )
    .await
    .unwrap();

    let published = sink.events().await;

    // Should have published: event 0 (success), event 1 (retry), event 2 (new)
    assert_eq!(published.len(), 3, "Should have exactly 3 events published");

    // Verify all 3 event IDs are present
    let published_ids: Vec<String> = published.iter().map(|e| e.id.id.clone()).collect();
    for i in 0..3 {
        let event_id = event_ids.get(i).expect("Should have event");
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }

    let (status, _) = stream.store().get_stream_state().await.unwrap();
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
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 5).await;

    // Event 0: success
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Event 1: fail - enters failover
    sink.fail_on_call(1);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    // Event 2: fail again - should retry event 1 first, then fail on event 2
    sink.fail_on_call(2);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )],
    )
    .await
    .unwrap();

    // Event 4: succeed - should replay events 1, 2, 3 and then process 4
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(4).expect("Should have event 4"),
            serde_json::json!({"seq": 5}),
        )],
    )
    .await
    .unwrap();

    let published = sink.events().await;

    // Should eventually publish all 5 unique events: 0, 1, 2, 3, 4
    // Note: Event 2 might be published twice (once when written, once during replay)
    // because checkpoint republish failed but event 2 publish succeeded
    assert!(
        published.len() >= 5 && published.len() <= 6,
        "Should have 5-6 events published (event 2 may be duplicated), got {}",
        published.len()
    );

    // Verify all 5 unique event IDs are present
    let published_ids: HashSet<String> = published.iter().map(|e| e.id.id.clone()).collect();
    assert_eq!(
        published_ids.len(),
        5,
        "Should have exactly 5 unique event IDs"
    );
    for i in 0..5 {
        let event_id = event_ids.get(i).expect("Should have event");
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }

    let (status, _) = stream.store().get_stream_state().await.unwrap();
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
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    // Insert a larger number of events to test recovery with bigger gaps
    let event_ids = insert_events_to_db(&db, 50).await;

    // Event 0: success
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Event 5: fail - creates gap of events 1-4
    sink.fail_on_call(1);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(5).expect("Should have event 5"),
            serde_json::json!({"seq": 6}),
        )],
    )
    .await
    .unwrap();

    // Event 49: succeed - should replay all missed events (5-48) plus new event
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(49).expect("Should have event 49"),
            serde_json::json!({"seq": 50}),
        )],
    )
    .await
    .unwrap();

    let published = sink.events().await;

    // Should have: event 0 (success) + events 5-48 (replayed) + event 49 (new) = 46 events
    assert_eq!(
        published.len(),
        46,
        "Should have exactly 46 events after gap recovery"
    );

    // Verify event 0 is present
    let published_ids: Vec<String> = published.iter().map(|e| e.id.id.clone()).collect();
    assert!(
        published_ids.contains(&event_ids.first().expect("Should have event 0").id),
        "Event 0 should be present"
    );

    // Verify events 5-49 are present
    for i in 5..50 {
        let event_id = event_ids.get(i).expect("Should have event");
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should recover to Healthy after large gap"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_recovery_does_not_hang_when_replay_exceeds_batch_size() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    // 250 events ensure replay window is larger than batch.max_size (100).
    let event_ids = insert_events_to_db(&db, 250).await;

    // Event 0 succeeds.
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Event 1 fails, entering failover mode.
    sink.fail_on_call(1);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    // Event 249 should trigger failover replay with a gap > batch size.
    sink.succeed_always();
    let recovery_result = tokio::time::timeout(
        StdDuration::from_secs(20),
        write_events(
            &stream,
            vec![make_event_with_id(
                table_id,
                event_ids.get(249).expect("Should have event 249"),
                serde_json::json!({"seq": 250}),
            )],
        ),
    )
    .await;

    assert!(
        recovery_result.is_ok(),
        "Failover recovery should not hang when replay exceeds batch size"
    );
    recovery_result.unwrap().unwrap();

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should return to Healthy after large replay recovery"
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
    write_events(
        &stream1,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Event 1: fail - enters failover
    sink.fail_on_call(1);
    write_events(
        &stream1,
        vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    // Drop the first stream (simulating restart)
    drop(stream1);

    // Create a new stream instance - should pick up failover state
    let stream2: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream after restart");

    // Verify it's in failover state
    let (status, _) = stream2.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Failover { .. }),
        "Stream should be in Failover state after restart"
    );

    // Event 2: succeed - should recover
    sink.succeed_always();
    write_events(
        &stream2,
        vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )],
    )
    .await
    .unwrap();

    let published = sink.events().await;

    // Should have recovered and published all 3 events
    assert_eq!(published.len(), 3, "Should have exactly 3 events published");

    // Verify all 3 event IDs are present
    let published_ids: Vec<String> = published.iter().map(|e| e.id.id.clone()).collect();
    for i in 0..3 {
        let event_id = event_ids.get(i).expect("Should have event");
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }

    let (status, _) = stream2.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should be Healthy after recovery"
    );
}

// Shutdown tests

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_succeeds_and_preserves_maintenance_schedule() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    // First create a stream to initialize the schema
    let stream1: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store.clone())
            .await
            .unwrap();

    // Set next_maintenance_at to 1 hour in the past
    let past_maintenance_time = chrono::Utc::now() - Duration::hours(1);
    stream1
        .store()
        .store_next_maintenance_at(past_maintenance_time)
        .await
        .unwrap();

    // Drop and recreate stream - this reads the past time from DB and runs maintenance
    drop(stream1);
    let stream2: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config.clone(), MemorySink::new(), store)
            .await
            .unwrap();

    // Verify maintenance ran and scheduled next run 24h from the past scheduled time
    let (_, next_maintenance) = stream2.store().get_stream_state().await.unwrap();
    let expected_next = past_maintenance_time + Duration::hours(24);

    assert_eq!(
        next_maintenance.timestamp(),
        expected_next.timestamp(),
        "Next maintenance should be scheduled 24h after the original scheduled time"
    );

    // Shutdown should complete successfully
    stream2.shutdown().await.unwrap();

    // Schedule should be preserved after shutdown
    let (_, next_after_shutdown) = stream2.store().get_stream_state().await.unwrap();

    assert_eq!(
        next_after_shutdown.timestamp(),
        expected_next.timestamp(),
        "Next maintenance schedule should be preserved after shutdown"
    );
}

// Flaky sink failover tests

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_flaky_sink_mid_replay_recovers() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 6).await;

    // Event 0: success
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Event 1: fail - enters failover (call 1)
    sink.fail_on_call(1);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Failover { .. }),
        "Stream should be in failover after sink failure"
    );

    // Next tick: checkpoint republish succeeds (call 2), but replay publish fails (call 3)
    sink.fail_on_call(3);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(5).expect("Should have event 5"),
            serde_json::json!({"seq": 6}),
        )],
    )
    .await
    .unwrap();

    // Should still be in failover because replay failed mid-way
    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Failover { .. }),
        "Stream should remain in failover after mid-replay failure"
    );

    // Next tick: everything succeeds - full recovery
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(5).expect("Should have event 5"),
            serde_json::json!({"seq": 6}),
        )],
    )
    .await
    .unwrap();

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should recover to Healthy after retry"
    );

    // Verify all unique event IDs were published
    let published = sink.events().await;
    let published_ids: HashSet<String> = published.iter().map(|e| e.id.id.clone()).collect();
    for event_id in &event_ids {
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_flaky_sink_checkpoint_publish_retries() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 4).await;

    // Event 0: success (call 0)
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )],
    )
    .await
    .unwrap();

    // Event 1: fail - enters failover (call 1)
    sink.fail_on_call(1);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(1).expect("Should have event 1"),
            serde_json::json!({"seq": 2}),
        )],
    )
    .await
    .unwrap();

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Failover { .. }),
        "Stream should be in failover after sink failure"
    );

    // Next tick: checkpoint republish fails (call 2) - sink still unavailable
    sink.fail_on_call(2);
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(3).expect("Should have event 3"),
            serde_json::json!({"seq": 4}),
        )],
    )
    .await
    .unwrap();

    // Should still be in failover
    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Failover { .. }),
        "Stream should remain in failover when checkpoint publish fails"
    );

    // Next tick: everything succeeds - full recovery
    sink.succeed_always();
    write_events(
        &stream,
        vec![make_event_with_id(
            table_id,
            event_ids.get(3).expect("Should have event 3"),
            serde_json::json!({"seq": 4}),
        )],
    )
    .await
    .unwrap();

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should recover to Healthy after checkpoint publish retry succeeds"
    );

    // Verify all unique event IDs were published
    let published = sink.events().await;
    let published_ids: HashSet<String> = published.iter().map(|e| e.id.id.clone()).collect();
    for event_id in &event_ids {
        assert!(
            published_ids.contains(&event_id.id),
            "Event {} should be in published events",
            event_id.id
        );
    }
}

// Batch splitting

/// Returns a stream config whose batches hold at most `max_size` events.
fn small_batch_config(db: &TestDatabase, max_size: usize) -> StreamConfig {
    let mut config = test_stream_config(db);
    config.batch.max_size = max_size;
    config
}

fn assert_all_published(published: &[TriggeredEvent], event_ids: &[EventIdentifier]) {
    let published_ids: HashSet<&str> = published.iter().map(|e| e.id.id.as_str()).collect();
    for event_id in event_ids {
        assert!(
            published_ids.contains(event_id.id.as_str()),
            "Event {} should be published",
            event_id.id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_splits_into_batches_of_max_size() {
    let db = TestDatabase::spawn().await;
    let config = small_batch_config(&db, 3);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;
    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config, sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 8).await;
    let events = event_ids
        .iter()
        .enumerate()
        .map(|(i, id)| make_event_with_id(table_id, id, serde_json::json!({"seq": i})))
        .collect();

    write_events(&stream, events).await.unwrap();

    // 3 + 3 + 2 events, published in order.
    assert_eq!(sink.call_count(), 3);
    let published_ids: Vec<String> = sink.events().await.into_iter().map(|e| e.id.id).collect();
    let expected_ids: Vec<String> = event_ids.iter().map(|e| e.id.clone()).collect();
    assert_eq!(published_ids, expected_ids);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_failure_on_middle_batch_loses_nothing() {
    let db = TestDatabase::spawn().await;
    let config = small_batch_config(&db, 3);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;
    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config, sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 9).await;
    let events = event_ids
        .iter()
        .enumerate()
        .map(|(i, id)| make_event_with_id(table_id, id, serde_json::json!({"seq": i})))
        .collect();

    // Calls: 0 = events 0-2, 1 = events 3-5 (fails), then the last batch replays from the
    // checkpoint (event 3) before publishing events 6-8.
    sink.fail_on_call(1);
    let status = write_events(&stream, events).await.unwrap();
    assert_eq!(status, DestinationWriteStatus::Durable);

    assert_all_published(&sink.events().await, &event_ids);
    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(
        matches!(status, StreamStatus::Healthy),
        "Stream should recover within the same write"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_failure_on_last_batch_is_replayed_by_next_write() {
    let db = TestDatabase::spawn().await;
    let config = small_batch_config(&db, 3);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;
    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config, sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 7).await;
    let events: Vec<_> = event_ids
        .iter()
        .enumerate()
        .map(|(i, id)| make_event_with_id(table_id, id, serde_json::json!({"seq": i})))
        .collect();
    let (first_write, second_write) = events.split_at(6);

    // The last batch (events 3-5) fails: the write is still durable because the events are
    // recovered from the events table.
    sink.fail_on_call(1);
    write_events(&stream, first_write.to_vec()).await.unwrap();
    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert_eq!(
        status,
        StreamStatus::Failover {
            checkpoint_event_id: event_ids.get(3).cloned().unwrap()
        }
    );

    write_events(&stream, second_write.to_vec()).await.unwrap();

    assert_all_published(&sink.events().await, &event_ids);
    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(matches!(status, StreamStatus::Healthy));
}

// Failover replay window tests

async fn published_ids(sink: &FailableSink) -> HashSet<String> {
    sink.events().await.into_iter().map(|e| e.id.id).collect()
}

/// Batches arrive in commit order, so a batch dropped during failover can hold events that sort
/// after the batch that ends failover.
#[tokio::test(flavor = "multi_thread")]
async fn test_failover_replays_dropped_batch_sorted_after_recovery_batch() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    // created_at of e0 < e1 < e2 < e3
    let e = insert_events_to_db(&db, 4).await;
    let write = |id: usize| {
        let event =
            make_event_with_id(table_id, e.get(id).unwrap(), serde_json::json!({"seq": id}));
        write_events(&stream, vec![event])
    };

    // [e0] published
    sink.succeed_always();
    write(0).await.unwrap();

    // [e1] fails and enters failover; [e3] is dropped because the checkpoint republish fails
    sink.fail_on_calls(1, 3);
    write(1).await.unwrap();
    write(3).await.unwrap();

    // [e2] ends failover; e3 sorts after it but must be replayed
    sink.succeed_always();
    write(2).await.unwrap();

    let published = published_ids(&sink).await;
    for (i, event) in e.iter().enumerate() {
        assert!(published.contains(&event.id), "e{i} should be published");
    }

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    assert!(matches!(status, StreamStatus::Healthy));
    assert_eq!(stream.store().get_failover_window_end().await, None);
}

/// Events of a failed batch can sort before the batch's first event.
#[tokio::test(flavor = "multi_thread")]
async fn test_failover_replays_failed_batch_events_before_its_first_event() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream");

    let e = insert_events_to_db(&db, 5).await;
    let event = |id: usize| {
        make_event_with_id(table_id, e.get(id).unwrap(), serde_json::json!({"seq": id}))
    };

    sink.succeed_always();
    write_events(&stream, vec![event(0)]).await.unwrap();

    // [e3, e1] fails: the window must start at e1, not at the batch's first event
    sink.fail_on_call(1);
    write_events(&stream, vec![event(3), event(1)])
        .await
        .unwrap();

    let (status, _) = stream.store().get_stream_state().await.unwrap();
    match status {
        StreamStatus::Failover {
            checkpoint_event_id,
        } => assert_eq!(checkpoint_event_id.id, e.get(1).unwrap().id),
        StreamStatus::Healthy => panic!("Expected Failover status"),
    }
    assert_eq!(
        stream
            .store()
            .get_failover_window_end()
            .await
            .map(|end| end.id),
        Some(e.get(3).unwrap().id.clone())
    );

    sink.succeed_always();
    write_events(&stream, vec![event(4)]).await.unwrap();

    let published = published_ids(&sink).await;
    for (i, event) in e.iter().enumerate() {
        assert!(published.contains(&event.id), "e{i} should be published");
    }
}

/// The replay window is durable: a restart while in failover still replays dropped batches.
#[tokio::test(flavor = "multi_thread")]
async fn test_failover_replays_dropped_batch_after_restart() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailableSink::new();
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let e = insert_events_to_db(&db, 4).await;
    let event = |id: usize| {
        make_event_with_id(table_id, e.get(id).unwrap(), serde_json::json!({"seq": id}))
    };

    {
        let stream: PgStream<FailableSink, PostgresStore> =
            PgStream::create(config.clone(), sink.clone(), store.clone())
                .await
                .expect("Failed to create PgStream");

        sink.succeed_always();
        write_events(&stream, vec![event(0)]).await.unwrap();

        // [e1] fails, [e3] is dropped
        sink.fail_on_calls(1, 3);
        write_events(&stream, vec![event(1)]).await.unwrap();
        write_events(&stream, vec![event(3)]).await.unwrap();
    }

    // Restart: a new instance loads the window from the database
    let stream: PgStream<FailableSink, PostgresStore> =
        PgStream::create(config.clone(), sink.clone(), store)
            .await
            .expect("Failed to create PgStream after restart");
    assert_eq!(
        stream
            .store()
            .get_failover_window_end()
            .await
            .map(|end| end.id),
        Some(e.get(3).unwrap().id.clone())
    );

    sink.succeed_always();
    write_events(&stream, vec![event(2)]).await.unwrap();

    let published = published_ids(&sink).await;
    for (i, event) in e.iter().enumerate() {
        assert!(published.contains(&event.id), "e{i} should be published");
    }
}
