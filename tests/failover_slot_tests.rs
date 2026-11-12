//! Integration tests for failover replication slots (`stream.failover`).
//!
//! Failover slots require Postgres 17+. On older servers these tests are skipped.

use std::time::Duration;

use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use postgres_stream::config::StreamConfig;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    TestDatabase, slot_failover, supports_failover_slots, test_stream_config_with_id,
    unique_pipeline_id,
};
use postgres_stream::types::SlotName;

/// Returns whether the events table finished its initial sync.
async fn table_synced(db: &TestDatabase, pipeline_id: u64) -> bool {
    let states: Vec<String> = sqlx::query_scalar(
        "select state::text from etl.replication_state where pipeline_id = $1 and is_current",
    )
    .bind(pipeline_id as i64)
    .fetch_all(&db.pool)
    .await
    .unwrap();

    !states.is_empty()
        && states
            .iter()
            .all(|state| state == "sync_done" || state == "ready")
}

/// Starts a pipeline, waits until the events table is synced, its slot has the expected
/// `failover` flag and the sink holds at least `expected_events` events, then shuts it down.
async fn run_pipeline_until_slot_failover(
    db: &TestDatabase,
    stream_config: &StreamConfig,
    sink: &MemorySink,
    expected_failover: bool,
    expected_events: usize,
) {
    let state_store = PostgresStore::new(stream_config.id, db.config.clone())
        .await
        .expect("Failed to create PostgresStore");
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);
    pipeline.start().await.expect("Failed to start pipeline");

    let slot_name = stream_config.id.slot_name();
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        while !table_synced(db, stream_config.id).await
            || slot_failover(&db.pool, &slot_name).await != Some(expected_failover)
            || sink.events().await.len() < expected_events
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    pipeline.shutdown();
    pipeline.wait().await.expect("Pipeline should shut down");

    assert!(
        result.is_ok(),
        "table should be synced, slot {slot_name} should exist with failover = \
         {expected_failover} (got {:?}) and the sink should hold {expected_events} events (got {})",
        slot_failover(&db.pool, &slot_name).await,
        sink.events().await.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slot_is_created_with_failover_when_enabled() {
    let db = TestDatabase::spawn().await;
    if !supports_failover_slots(&db.pool).await {
        eprintln!("skipping: failover slots require Postgres 17+");
        return;
    }

    let mut stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    stream_config.failover = true;

    run_pipeline_until_slot_failover(&db, &stream_config, &MemorySink::new(), true, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slot_is_created_without_failover_by_default() {
    let db = TestDatabase::spawn().await;
    if !supports_failover_slots(&db.pool).await {
        eprintln!("skipping: failover slots require Postgres 17+");
        return;
    }

    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    assert!(!stream_config.failover);

    run_pipeline_until_slot_failover(&db, &stream_config, &MemorySink::new(), false, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_existing_slot_is_upgraded_to_failover() {
    let db = TestDatabase::spawn().await;
    if !supports_failover_slots(&db.pool).await {
        eprintln!("skipping: failover slots require Postgres 17+");
        return;
    }

    db.ensure_today_partition().await;
    let mut stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let sink = MemorySink::new();

    // An existing deployment created its slot without failover.
    run_pipeline_until_slot_failover(&db, &stream_config, &sink, false, 0).await;

    // Written while the pipeline is down: only delivered if the existing slot is reused.
    sqlx::query("insert into pgstream.events (payload, stream_id) values ($1, $2)")
        .bind(serde_json::json!({"written_while_down": true}))
        .bind(stream_config.id as i64)
        .execute(&db.pool)
        .await
        .unwrap();

    // Enabling failover alters the existing slot instead of recreating it.
    stream_config.failover = true;
    run_pipeline_until_slot_failover(&db, &stream_config, &sink, true, 1).await;

    assert_eq!(
        sink.events()
            .await
            .first()
            .map(|event| event.payload.clone()),
        Some(serde_json::json!({"written_while_down": true}))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_is_rejected_before_postgres_17() {
    let db = TestDatabase::spawn().await;
    if supports_failover_slots(&db.pool).await {
        eprintln!("skipping: Postgres 17+ supports failover slots");
        return;
    }

    let mut stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    stream_config.failover = true;

    let state_store = PostgresStore::new(stream_config.id, db.config.clone())
        .await
        .expect("Failed to create PostgresStore");
    let pgstream = PgStream::create(
        stream_config.clone(),
        MemorySink::new(),
        state_store.clone(),
    )
    .await
    .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);

    let result = match pipeline.start().await {
        Err(err) => Err(err),
        Ok(()) => tokio::time::timeout(Duration::from_secs(30), pipeline.wait())
            .await
            .expect("Pipeline should fail instead of running"),
    };

    let err = result.expect_err("failover should be rejected before Postgres 17");
    assert!(
        err.to_string().contains("PostgreSQL 17"),
        "unexpected error: {err}"
    );
}
