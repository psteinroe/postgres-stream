//! Integration tests for slot recovery using production code paths.
//!
//! These tests verify the system's behavior when replication slots are invalidated
//! and test the recovery mechanisms.
//!
//! Note: These tests use `ALTER SYSTEM` commands which affect the entire Postgres
//! server. They acquire an exclusive lock to ensure they don't run concurrently
//! with other tests.

use std::time::Duration;

use etl::store::both::postgres::PostgresStore;
use postgres_stream::migrations::migrate_etl;
use postgres_stream::recovery::handle_slot_recovery;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    TestDatabase, acquire_exclusive_test_lock, test_stream_config_with_id, unique_pipeline_id,
};

/// Test that a pipeline fails when attempting to use an invalidated slot.
#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_fails_on_invalidated_slot() {
    // Acquire exclusive lock since we modify system settings
    let _lock = acquire_exclusive_test_lock().await;

    let db = TestDatabase::spawn().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
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
        shutdown_tx
            .shutdown()
            .expect("Failed to send shutdown signal");
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

    // Generate ~50MB of WAL to ensure invalidation
    for batch in 0..50 {
        for _ in 0..10 {
            sqlx::query("insert into wal_bloat (data) select decode(repeat('ab', 50000), 'hex') from generate_series(1, 10)")
                .execute(&db.pool)
                .await
                .unwrap();
        }

        // Periodically switch WAL and checkpoint to trigger cleanup
        if batch % 10 == 9 {
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

    // Reset system settings
    sqlx::query("alter system reset max_slot_wal_keep_size")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await
        .unwrap();

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

/// Test that handle_slot_recovery sets up failover checkpoint and allows pipeline restart.
#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_recovers_from_invalidated_slot() {
    // Acquire exclusive lock since we modify system settings
    let _lock = acquire_exclusive_test_lock().await;

    let db = TestDatabase::spawn().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let pipeline_id = stream_config.id;
    let slot_name = format!("supabase_etl_apply_{pipeline_id}");

    // Run migrations
    migrate_etl(&db.config)
        .await
        .expect("Failed to run ETL migrations");

    // Create a subscription so we generate events
    db.ensure_today_partition().await;

    // Create a shared sink to track events across pipeline restarts
    let sink = MemorySink::new();

    // Step 1: Start pipeline FIRST, then insert events
    // (events must be inserted AFTER slot creation to be captured via replication)
    {
        let state_store = PostgresStore::new(pipeline_id, db.config.clone());
        let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
            .await
            .expect("Failed to create PgStream");

        let pipeline_config: etl::config::PipelineConfig = stream_config.clone().into();
        let mut pipeline = etl::pipeline::Pipeline::new(pipeline_config, state_store, pgstream);

        pipeline.start().await.expect("Failed to start pipeline");

        // Small delay to ensure slot is created
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now insert events - these will be captured by the slot
        for i in 0..5 {
            sqlx::query("insert into pgstream.events (id, payload, stream_id, created_at, lsn) values (gen_random_uuid(), $1, $2, now(), pg_current_wal_lsn())")
                .bind(serde_json::json!({"before_shutdown": i}))
                .bind(pipeline_id as i64)
                .execute(&db.pool)
                .await
                .unwrap();
        }

        // Wait for events to be processed
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Graceful shutdown
        let shutdown_tx = pipeline.shutdown_tx();
        shutdown_tx
            .shutdown()
            .expect("Failed to send shutdown signal");
        pipeline.wait().await.expect("Failed to wait for pipeline");
    }

    // Step 2: Insert more events (these will need to be replayed after recovery)
    for i in 0..5 {
        sqlx::query("insert into pgstream.events (id, payload, stream_id, created_at, lsn) values (gen_random_uuid(), $1, $2, now(), pg_current_wal_lsn())")
            .bind(serde_json::json!({"after_stop_before_invalidation": i}))
            .bind(pipeline_id as i64)
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
        sqlx::query("insert into pgstream.events (id, payload, stream_id, created_at, lsn) values (gen_random_uuid(), $1, $2, now(), pg_current_wal_lsn())")
            .bind(serde_json::json!({"after_invalidation": i}))
            .bind(pipeline_id as i64)
            .execute(&db.pool)
            .await
            .unwrap();
    }

    // Step 5: Call slot recovery to set up failover checkpoint and drop the invalid slot
    handle_slot_recovery(&db.pool, pipeline_id)
        .await
        .expect("Slot recovery should succeed");

    // Verify the checkpoint was saved to the database
    let saved_checkpoint: (Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT failover_checkpoint_id, failover_checkpoint_ts FROM pgstream.streams WHERE id = $1",
    )
    .bind(pipeline_id as i64)
    .fetch_one(&db.pool)
    .await
    .expect("Should find stream row");

    assert!(
        saved_checkpoint.0.is_some(),
        "Checkpoint ID should be saved"
    );
    assert!(
        saved_checkpoint.1.is_some(),
        "Checkpoint timestamp should be saved"
    );

    let checkpoint_id = saved_checkpoint.0.unwrap();
    let checkpoint_ts = saved_checkpoint.1.unwrap();

    // Verify the checkpoint event exists in events table
    let checkpoint_event_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pgstream.events WHERE id::text = $1 AND created_at = $2)",
    )
    .bind(&checkpoint_id)
    .bind(checkpoint_ts)
    .fetch_one(&db.pool)
    .await
    .expect("Query should succeed");

    assert!(
        checkpoint_event_exists,
        "Checkpoint event should exist in events table"
    );

    // Step 6: Restart pipeline - verify it can start with a new slot
    {
        let state_store = PostgresStore::new(pipeline_id, db.config.clone());
        let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
            .await
            .expect("Failed to create PgStream");

        let pipeline_config: etl::config::PipelineConfig = stream_config.into();
        let mut pipeline = etl::pipeline::Pipeline::new(pipeline_config, state_store, pgstream);

        // This should succeed - the slot was dropped and a new one will be created
        pipeline
            .start()
            .await
            .expect("Pipeline should start after slot recovery");

        // Wait for slot to be created
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Graceful shutdown
        let shutdown_tx = pipeline.shutdown_tx();
        shutdown_tx.shutdown().expect("Failed to shutdown");
        pipeline.wait().await.expect("Failed to wait");
    }

    // Verify the new slot was created (not the invalidated one)
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

    // Verify the stream is in failover mode with a valid checkpoint
    let stream_state: (Option<String>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT failover_checkpoint_id, failover_checkpoint_ts FROM pgstream.streams WHERE id = $1",
    )
    .bind(pipeline_id as i64)
    .fetch_one(&db.pool)
    .await
    .expect("Should find stream row");

    assert!(
        stream_state.0.is_some() && stream_state.1.is_some(),
        "Failover checkpoint should be set after slot recovery"
    );

    // Cleanup
    sqlx::query("alter system reset max_slot_wal_keep_size")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("select pg_reload_conf()")
        .execute(&db.pool)
        .await
        .unwrap();
}
