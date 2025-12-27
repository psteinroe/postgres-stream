//! Integration tests for slot recovery using production code paths.
//!
//! These tests verify the system's behavior when replication slots are invalidated
//! and test the recovery mechanisms.

use std::time::Duration;

use etl::store::both::postgres::PostgresStore;
use postgres_stream::migrations::migrate_etl;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{TestDatabase, test_stream_config};

/// Test that demonstrates the current failure mode when a slot is invalidated.
///
/// This test:
/// 1. Starts a pipeline (creates replication slot)
/// 2. Shuts down the pipeline gracefully
/// 3. Invalidates the slot while pipeline is stopped
/// 4. Attempts to restart the pipeline
/// 5. Expects the restart to fail (current behavior) or recover (after implementation)
#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_fails_on_invalidated_slot() {
    let db = TestDatabase::spawn().await;
    let stream_config = test_stream_config(&db);
    let pipeline_id = stream_config.id;

    // The slot name follows the etl crate's naming convention
    let slot_name = format!("supabase_etl_apply_{pipeline_id}");

    // Step 1: Run ETL migrations
    migrate_etl(&db.config)
        .await
        .expect("Failed to run ETL migrations");

    // Step 2: Create and start a pipeline to establish the replication slot
    {
        let state_store = PostgresStore::new(pipeline_id, db.config.clone());
        let sink = MemorySink::new();
        let pgstream = PgStream::create(stream_config.clone(), sink, state_store.clone())
            .await
            .expect("Failed to create PgStream");

        let pipeline_config: etl::config::PipelineConfig = stream_config.clone().into();
        let mut pipeline = etl::pipeline::Pipeline::new(pipeline_config, state_store, pgstream);

        // Start the pipeline - this creates the replication slot
        pipeline.start().await.expect("Failed to start pipeline");

        // Give it a moment to establish the slot
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify slot was created
        let slot_exists: bool = sqlx::query_scalar(&format!(
            "select exists(select 1 from pg_replication_slots where slot_name = '{slot_name}')"
        ))
        .fetch_one(&db.pool)
        .await
        .unwrap();

        assert!(slot_exists, "Replication slot should be created");

        // Gracefully shutdown the pipeline
        let shutdown_tx = pipeline.shutdown_tx();
        shutdown_tx.shutdown().expect("Failed to send shutdown");
        pipeline.wait().await.expect("Failed to wait for pipeline");
    }

    // Step 3: Verify slot still exists after shutdown
    let slot_exists: bool = sqlx::query_scalar(&format!(
        "select exists(select 1 from pg_replication_slots where slot_name = '{slot_name}')"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert!(slot_exists, "Slot should persist after graceful shutdown");

    // Step 4: Invalidate the slot by generating WAL
    sqlx::query("alter system set max_slot_wal_keep_size = '1MB'")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await
        .unwrap();

    // Generate WAL to invalidate the slot - need enough to exceed 1MB
    sqlx::query("create table wal_bloat (id serial, data bytea)")
        .execute(&db.pool)
        .await
        .unwrap();

    // Generate ~10MB of WAL to ensure invalidation
    for batch in 0..20 {
        for _ in 0..10 {
            sqlx::query("insert into wal_bloat (data) select decode(repeat('ab', 50000), 'hex') from generate_series(1, 10)")
                .execute(&db.pool)
                .await
                .unwrap();
        }

        // Periodically switch WAL and checkpoint to trigger cleanup
        if batch % 5 == 4 {
            let _: Option<String> = sqlx::query_scalar("select pg_switch_wal()::text")
                .fetch_one(&db.pool)
                .await
                .unwrap();
            sqlx::query("checkpoint").execute(&db.pool).await.unwrap();
        }
    }

    // Final WAL switch and checkpoint
    let _: Option<String> = sqlx::query_scalar("select pg_switch_wal()::text")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    sqlx::query("checkpoint").execute(&db.pool).await.unwrap();

    // Give postgres a moment to clean up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify slot is now invalidated
    let wal_status: String = sqlx::query_scalar(&format!(
        "select wal_status from pg_replication_slots where slot_name = '{slot_name}'"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(
        wal_status, "lost",
        "Slot should be invalidated (wal_status=lost)"
    );

    // Verify confirmed_flush_lsn is preserved for recovery
    let confirmed_lsn: Option<String> = sqlx::query_scalar(&format!(
        "select confirmed_flush_lsn::text from pg_replication_slots where slot_name = '{slot_name}'"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert!(
        confirmed_lsn.is_some(),
        "confirmed_flush_lsn should be preserved for recovery"
    );

    // Step 5: Try to restart the pipeline with the invalidated slot
    let state_store = PostgresStore::new(pipeline_id, db.config.clone());
    let sink = MemorySink::new();
    let pgstream = PgStream::create(stream_config.clone(), sink, state_store.clone())
        .await
        .expect("Failed to create PgStream");

    let pipeline_config: etl::config::PipelineConfig = stream_config.into();
    let mut pipeline = etl::pipeline::Pipeline::new(pipeline_config, state_store, pgstream);

    // This should fail with a slot invalidation error
    let start_result = pipeline.start().await;

    // Clean up settings
    let _ = sqlx::query("alter system reset max_slot_wal_keep_size")
        .execute(&db.pool)
        .await;
    let _ = sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await;

    // Assert the expected behavior
    match start_result {
        Ok(()) => {
            // If start succeeded, wait a bit and check if it fails during operation
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Get shutdown handle before consuming pipeline
            let shutdown_tx = pipeline.shutdown_tx();

            // The pipeline might have started but should fail when trying to use the slot
            let wait_result = tokio::time::timeout(Duration::from_secs(5), pipeline.wait()).await;

            match wait_result {
                Ok(Ok(())) => {
                    panic!(
                        "Pipeline should have failed due to invalidated slot, but completed successfully"
                    );
                }
                Ok(Err(e)) => {
                    let error_msg = e.to_string().to_lowercase();
                    assert!(
                        error_msg.contains("slot") || error_msg.contains("replication"),
                        "Expected slot-related error, got: {e}"
                    );
                }
                Err(_) => {
                    // Timeout - pipeline is still running, need to shut it down
                    let _ = shutdown_tx.shutdown();
                    panic!(
                        "Pipeline did not fail within timeout - expected failure due to invalidated slot"
                    );
                }
            }
        }
        Err(e) => {
            let error_msg = e.to_string().to_lowercase();
            // This is expected - the pipeline should fail because the slot is invalidated
            assert!(
                error_msg.contains("slot")
                    || error_msg.contains("replication")
                    || error_msg.contains("can no longer"),
                "Expected slot-related error, got: {e}"
            );
        }
    }
}

/// Test that the system recovers automatically when a slot is invalidated.
///
/// This test:
/// 1. Creates some events in the database
/// 2. Starts a pipeline and processes some events
/// 3. Shuts down the pipeline
/// 4. Invalidates the slot
/// 5. Restarts the pipeline
/// 6. Expects the pipeline to automatically recover and replay missed events
///
/// This test is expected to FAIL until we implement automatic slot recovery.
/// Once implemented, the pipeline should:
/// - Detect the invalidated slot
/// - Query confirmed_flush_lsn from pg_replication_slots
/// - Find events after that LSN using the lsn column in pgstream.events
/// - Replay those events to the sink
/// - Create a new slot and continue normally
#[tokio::test(flavor = "multi_thread")]
#[ignore = "Slot recovery not yet implemented - this test documents expected behavior"]
async fn test_pipeline_recovers_automatically_from_invalidated_slot() {
    let db = TestDatabase::spawn().await;
    let stream_config = test_stream_config(&db);
    let pipeline_id = stream_config.id;
    let slot_name = format!("supabase_etl_apply_{pipeline_id}");

    // Run migrations
    migrate_etl(&db.config)
        .await
        .expect("Failed to run ETL migrations");

    // Create a subscription so we generate events
    db.ensure_today_partition().await;

    // Insert some initial events that will be processed before slot invalidation
    for i in 0..5 {
        sqlx::query("insert into pgstream.events (id, payload, stream_id, created_at) values (gen_random_uuid(), $1, 1, now())")
            .bind(serde_json::json!({"before_invalidation": i}))
            .execute(&db.pool)
            .await
            .unwrap();
    }

    // Create a shared sink to track events across pipeline restarts
    let sink = MemorySink::new();

    // Step 1: Start pipeline and process initial events
    {
        let state_store = PostgresStore::new(pipeline_id, db.config.clone());
        let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
            .await
            .expect("Failed to create PgStream");

        let pipeline_config: etl::config::PipelineConfig = stream_config.clone().into();
        let mut pipeline = etl::pipeline::Pipeline::new(pipeline_config, state_store, pgstream);

        pipeline.start().await.expect("Failed to start pipeline");

        // Wait for initial events to be processed
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Graceful shutdown
        let shutdown_tx = pipeline.shutdown_tx();
        shutdown_tx.shutdown().expect("Failed to shutdown");
        pipeline.wait().await.expect("Failed to wait");
    }

    // Step 2: Insert more events (these will need to be replayed after recovery)
    for i in 0..5 {
        sqlx::query("insert into pgstream.events (id, payload, stream_id, created_at) values (gen_random_uuid(), $1, 1, now())")
            .bind(serde_json::json!({"after_stop_before_invalidation": i}))
            .execute(&db.pool)
            .await
            .unwrap();
    }

    // Step 3: Invalidate the slot
    sqlx::query("alter system set max_slot_wal_keep_size = '1MB'")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await
        .unwrap();

    sqlx::query("create table wal_bloat (data bytea)")
        .execute(&db.pool)
        .await
        .unwrap();

    for _ in 0..100 {
        sqlx::query("insert into wal_bloat select decode(repeat('ab', 50000), 'hex') from generate_series(1, 10)")
            .execute(&db.pool)
            .await
            .unwrap();
    }

    let _: Option<String> = sqlx::query_scalar("select pg_switch_wal()::text")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    sqlx::query("checkpoint").execute(&db.pool).await.unwrap();

    // Verify slot is invalidated
    let wal_status: String = sqlx::query_scalar(&format!(
        "select wal_status from pg_replication_slots where slot_name = '{slot_name}'"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(wal_status, "lost");

    // Step 4: Insert more events after invalidation (should also be replayed)
    for i in 0..5 {
        sqlx::query("insert into pgstream.events (id, payload, stream_id, created_at) values (gen_random_uuid(), $1, 1, now())")
            .bind(serde_json::json!({"after_invalidation": i}))
            .execute(&db.pool)
            .await
            .unwrap();
    }

    // Step 5: Restart pipeline - it should recover automatically
    {
        let state_store = PostgresStore::new(pipeline_id, db.config.clone());
        let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
            .await
            .expect("Failed to create PgStream");

        let pipeline_config: etl::config::PipelineConfig = stream_config.into();
        let mut pipeline = etl::pipeline::Pipeline::new(pipeline_config, state_store, pgstream);

        // This should succeed after recovery is implemented
        pipeline
            .start()
            .await
            .expect("Pipeline should recover from invalidated slot");

        // Wait for recovery and processing
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Graceful shutdown
        let shutdown_tx = pipeline.shutdown_tx();
        shutdown_tx.shutdown().expect("Failed to shutdown");
        pipeline.wait().await.expect("Failed to wait");
    }

    // Clean up
    let _ = sqlx::query("alter system reset max_slot_wal_keep_size")
        .execute(&db.pool)
        .await;
    let _ = sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await;

    // Step 6: Verify all events were processed
    let final_events = sink.events().await;

    // We should have:
    // - 5 events before invalidation (already processed)
    // - 5 events after stop but before invalidation (need replay)
    // - 5 events after invalidation (need replay)
    // Total: 15 events
    assert_eq!(
        final_events.len(),
        15,
        "All events should be processed after recovery. \
         Got {} events, expected 15 (5 before + 5 during + 5 after invalidation)",
        final_events.len()
    );

    // Verify the new slot was created
    let new_slot_exists: bool = sqlx::query_scalar(&format!(
        "select exists(select 1 from pg_replication_slots where slot_name = '{slot_name}' and wal_status != 'lost')"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert!(
        new_slot_exists,
        "A new healthy slot should be created after recovery"
    );
}

/// Test that verifies we can detect a slot invalidation and get the recovery LSN.
///
/// This is a simpler test that just verifies we can query the slot status
/// and get the information needed for recovery.
#[tokio::test(flavor = "multi_thread")]
async fn test_can_detect_slot_invalidation_and_get_recovery_info() {
    let db = TestDatabase::spawn().await;

    let slot_name = "test_recovery_info_slot";

    // Create a slot
    sqlx::query(&format!(
        "select pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
    ))
    .execute(&db.pool)
    .await
    .unwrap();

    // Invalidate it
    sqlx::query("alter system set max_slot_wal_keep_size = '1MB'")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await
        .unwrap();

    sqlx::query("create table wal_gen (data bytea)")
        .execute(&db.pool)
        .await
        .unwrap();

    for _ in 0..100 {
        sqlx::query("insert into wal_gen select decode(repeat('ab', 50000), 'hex') from generate_series(1, 10)")
            .execute(&db.pool)
            .await
            .unwrap();
    }

    let _: Option<String> = sqlx::query_scalar("select pg_switch_wal()::text")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    sqlx::query("checkpoint").execute(&db.pool).await.unwrap();

    // Query slot status - this is what our recovery code will need to do
    let slot_info: (String, Option<String>) = sqlx::query_as(&format!(
        "select wal_status, confirmed_flush_lsn::text from pg_replication_slots where slot_name = '{slot_name}'"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(slot_info.0, "lost", "Slot should be invalidated");
    assert!(
        slot_info.1.is_some(),
        "confirmed_flush_lsn should be preserved for recovery"
    );

    // Clean up
    let _ = sqlx::query("alter system reset max_slot_wal_keep_size")
        .execute(&db.pool)
        .await;
    let _ = sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await;
    let _ = sqlx::query(&format!("select pg_drop_replication_slot('{slot_name}')"))
        .execute(&db.pool)
        .await;
}
