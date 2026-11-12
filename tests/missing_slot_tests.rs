//! Integration tests for recovering a replication slot that went missing after the events
//! table finished its initial sync, e.g. after a database failover to a standby without the slot.
//!
//! etl would create a new slot at the current WAL position and skip everything committed while
//! the slot was missing. These tests drive the same code paths as `core.rs`: the
//! [`SlotGuardStore`] stops the pipeline instead, and [`recover_lost_slot`] replays the missed
//! events.

use std::future::Future;
use std::time::Duration;

use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use postgres_stream::config::StreamConfig;
use postgres_stream::core::recover_lost_slot;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::slot_guard::SlotGuardStore;
use postgres_stream::slot_recovery::{SlotLoss, detect_slot_loss, record_slot_restart_lsn};
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    TestDatabase, acquire_shared_test_lock, test_stream_config_with_id, unique_pipeline_id,
};
use postgres_stream::types::SlotName;
use serde_json::json;
use sqlx::PgPool;

type Store = SlotGuardStore<PostgresStore>;
type TestPipeline = Pipeline<Store, PgStream<MemorySink, Store>>;

/// Polls `check` until it returns true, failing the test after 30 seconds.
async fn wait_for<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..150 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Starts a pipeline with the same store setup as `core.rs`.
async fn start_pipeline(
    db: &TestDatabase,
    stream_config: &StreamConfig,
    sink: &MemorySink,
) -> TestPipeline {
    let state_store = SlotGuardStore::new(
        PostgresStore::new(stream_config.id, db.config.clone())
            .await
            .expect("Failed to create PostgresStore"),
    );
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);
    pipeline.start().await.expect("Failed to start pipeline");
    pipeline
}

async fn stop_pipeline(pipeline: TestPipeline) {
    pipeline.shutdown();
    pipeline.wait().await.expect("Pipeline should shut down");
}

/// Inserts an event with the column defaults and returns its lsn.
async fn insert_event(pool: &PgPool, stream_id: u64, payload: serde_json::Value) -> String {
    sqlx::query_scalar(
        "insert into pgstream.events (payload, stream_id) values ($1, $2) returning lsn::text",
    )
    .bind(payload)
    .bind(stream_id as i64)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn delivered(sink: &MemorySink, payload: serde_json::Value) -> bool {
    sink.events()
        .await
        .iter()
        .any(|event| event.payload == payload)
}

async fn tables_synced(pool: &PgPool, stream_id: u64) -> bool {
    let states: Vec<String> = sqlx::query_scalar(
        "select state::text from etl.replication_state where pipeline_id = $1 and is_current",
    )
    .bind(stream_id as i64)
    .fetch_all(pool)
    .await
    .unwrap();

    !states.is_empty()
        && states
            .iter()
            .all(|state| state == "sync_done" || state == "ready")
}

async fn apply_progress(pool: &PgPool, stream_id: u64) -> Option<String> {
    sqlx::query_scalar(
        "select flush_lsn::text from etl.replication_progress
         where pipeline_id = $1 and worker_type = 'apply'",
    )
    .bind(stream_id as i64)
    .fetch_optional(pool)
    .await
    .unwrap()
}

async fn slot_exists(pool: &PgPool, stream_id: u64) -> bool {
    sqlx::query_scalar("select exists(select 1 from pg_replication_slots where slot_name = $1)")
        .bind(stream_id.slot_name())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn drop_slot(pool: &PgPool, stream_id: u64) {
    sqlx::query("select pg_drop_replication_slot($1)")
        .bind(stream_id.slot_name())
        .execute(pool)
        .await
        .unwrap();
}

/// Generates more WAL than the replay lower bound's `wal_buffers` margin.
async fn generate_wal(pool: &PgPool) {
    sqlx::query("create table if not exists wal_filler (data text)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "insert into wal_filler select md5(random()::text) from generate_series(1, 200000)",
    )
    .execute(pool)
    .await
    .unwrap();
}

/// Starts a pipeline, waits until the events table is synced and `payload` was delivered.
async fn run_until_delivered(
    db: &TestDatabase,
    stream_config: &StreamConfig,
    sink: &MemorySink,
    payload: serde_json::Value,
) -> TestPipeline {
    let pipeline = start_pipeline(db, stream_config, sink).await;
    wait_for("initial sync", || tables_synced(&db.pool, stream_config.id)).await;
    insert_event(&db.pool, stream_config.id, payload.clone()).await;
    wait_for("delivery", || delivered(sink, payload.clone())).await;
    pipeline
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_slot_while_synced_replays_events_written_while_down() {
    let _lock = acquire_shared_test_lock().await;
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let stream_id = stream_config.id;
    let sink = MemorySink::new();

    let pipeline = run_until_delivered(&db, &stream_config, &sink, json!({"before": true})).await;
    wait_for("apply progress", || async {
        apply_progress(&db.pool, stream_id).await.is_some()
    })
    .await;
    stop_pipeline(pipeline).await;

    insert_event(&db.pool, stream_id, json!({"while_down": true})).await;
    drop_slot(&db.pool, stream_id).await;

    assert_eq!(
        detect_slot_loss(&db.pool, stream_id).await.unwrap(),
        Some(SlotLoss::Missing)
    );
    recover_lost_slot(&db.pool, stream_id)
        .await
        .expect("Recovery should use etl's apply progress");
    assert_eq!(detect_slot_loss(&db.pool, stream_id).await.unwrap(), None);

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    wait_for("initial sync of the new slot", || {
        tables_synced(&db.pool, stream_id)
    })
    .await;
    insert_event(&db.pool, stream_id, json!({"after": true})).await;
    wait_for(
        "replay of the event written while the slot was missing",
        || delivered(&sink, json!({"while_down": true})),
    )
    .await;
    wait_for("delivery after recovery", || {
        delivered(&sink, json!({"after": true}))
    })
    .await;
    stop_pipeline(pipeline).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slot_lost_while_running_stops_pipeline_and_recovers() {
    let _lock = acquire_shared_test_lock().await;
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let stream_id = stream_config.id;
    let sink = MemorySink::new();

    let pipeline = run_until_delivered(&db, &stream_config, &sink, json!({"before": true})).await;
    record_slot_restart_lsn(&db.pool, stream_id).await.unwrap();

    // Lose the slot under the running pipeline, like a failover to a standby without it.
    wait_for("dropping the active slot", || async {
        sqlx::query(
            "select pg_terminate_backend(active_pid) from pg_replication_slots
             where slot_name = $1 and active_pid is not null",
        )
        .bind(stream_id.slot_name())
        .execute(&db.pool)
        .await
        .unwrap();
        sqlx::query("select pg_drop_replication_slot($1)")
            .bind(stream_id.slot_name())
            .execute(&db.pool)
            .await
            .is_ok()
    })
    .await;
    insert_event(&db.pool, stream_id, json!({"while_lost": true})).await;

    // etl reconnects and finds the slot missing. Instead of creating a new slot that skips
    // `while_lost`, the pipeline stops.
    let err = tokio::time::timeout(Duration::from_secs(60), pipeline.wait())
        .await
        .expect("Pipeline should stop")
        .expect_err("Pipeline should fail instead of replacing the slot");
    assert!(
        !slot_exists(&db.pool, stream_id).await,
        "no new slot may be created before recovery, pipeline error: {err}"
    );
    assert_eq!(
        detect_slot_loss(&db.pool, stream_id).await.unwrap(),
        Some(SlotLoss::Missing)
    );

    recover_lost_slot(&db.pool, stream_id)
        .await
        .expect("Recovery should succeed");

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    wait_for("initial sync of the new slot", || {
        tables_synced(&db.pool, stream_id)
    })
    .await;
    insert_event(&db.pool, stream_id, json!({"after": true})).await;
    wait_for(
        "replay of the event written while the slot was lost",
        || delivered(&sink, json!({"while_lost": true})),
    )
    .await;
    stop_pipeline(pipeline).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_slot_without_durable_position_fails() {
    let _lock = acquire_shared_test_lock().await;
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let stream_id = stream_config.id;
    let sink = MemorySink::new();

    let pipeline = run_until_delivered(&db, &stream_config, &sink, json!({"before": true})).await;
    stop_pipeline(pipeline).await;

    // A pipeline that never persisted its progress and whose restart_lsn was never recorded.
    sqlx::query("delete from etl.replication_progress where pipeline_id = $1")
        .bind(stream_id as i64)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("update pgstream.streams set slot_restart_lsn = null where id = $1")
        .bind(stream_id as i64)
        .execute(&db.pool)
        .await
        .unwrap();
    drop_slot(&db.pool, stream_id).await;

    let err = recover_lost_slot(&db.pool, stream_id)
        .await
        .expect_err("Recovery must not guess a replay position");
    let message = err.to_string();
    assert!(
        message.contains("without a durable replication position")
            && message.contains("slot_recovery_lsn"),
        "error should explain how to continue: {message}"
    );

    // Nothing was changed, so the next start fails the same way.
    assert_eq!(
        detect_slot_loss(&db.pool, stream_id).await.unwrap(),
        Some(SlotLoss::Missing)
    );

    // The guard keeps etl from creating a new slot that would skip the missed events.
    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    let result = tokio::time::timeout(Duration::from_secs(60), pipeline.wait())
        .await
        .expect("Pipeline should stop");
    assert!(result.is_err(), "pipeline must not start without its slot");
    assert!(!slot_exists(&db.pool, stream_id).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_slot_recovery_replays_transactions_committed_after_the_confirmed_position() {
    let _lock = acquire_shared_test_lock().await;
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let stream_id = stream_config.id;
    let sink = MemorySink::new();

    let pipeline = run_until_delivered(&db, &stream_config, &sink, json!({"ready": true})).await;
    // Retention would remove it eventually. Without it, the earliest event visible to recovery
    // is `long_tx`.
    sqlx::query("delete from pgstream.events where payload = $1")
        .bind(json!({"ready": true}))
        .execute(&db.pool)
        .await
        .unwrap();

    // Still running during recovery and committed while the new slot is created, so neither
    // recovery nor the new slot sees it. It is the earliest event.
    let mut running_tx = db.pool.acquire().await.unwrap().detach();
    sqlx::query("begin").execute(&mut running_tx).await.unwrap();
    sqlx::query("insert into pgstream.events (payload, stream_id) values ($1, $2)")
        .bind(json!({"running_during_recovery": true}))
        .bind(stream_id as i64)
        .execute(&mut running_tx)
        .await
        .unwrap();

    // A long transaction inserts an event, then the slot confirms a position far past it.
    let mut long_tx = db.pool.acquire().await.unwrap().detach();
    sqlx::query("begin").execute(&mut long_tx).await.unwrap();
    let long_tx_lsn: String = sqlx::query_scalar(
        "insert into pgstream.events (payload, stream_id) values ($1, $2) returning lsn::text",
    )
    .bind(json!({"long_tx": true}))
    .bind(stream_id as i64)
    .fetch_one(&mut long_tx)
    .await
    .unwrap();

    generate_wal(&db.pool).await;
    insert_event(&db.pool, stream_id, json!({"confirmed": true})).await;
    wait_for("delivery", || delivered(&sink, json!({"confirmed": true}))).await;
    wait_for("confirmed position past the margin", || async {
        let past: Option<bool> = sqlx::query_scalar(
            "select confirmed_flush_lsn - $2::pg_lsn
                    > 2 * pg_size_bytes(current_setting('wal_buffers'))
             from pg_replication_slots where slot_name = $1",
        )
        .bind(stream_id.slot_name())
        .bind(&long_tx_lsn)
        .fetch_optional(&db.pool)
        .await
        .unwrap()
        .flatten();
        past == Some(true)
    })
    .await;
    record_slot_restart_lsn(&db.pool, stream_id).await.unwrap();
    stop_pipeline(pipeline).await;

    // Commits after the confirmed position, then the slot is lost.
    sqlx::query("commit").execute(&mut long_tx).await.unwrap();

    insert_event(&db.pool, stream_id, json!({"while_down": true})).await;
    drop_slot(&db.pool, stream_id).await;
    recover_lost_slot(&db.pool, stream_id)
        .await
        .expect("Recovery should succeed");

    // Creating the new slot waits for the open transaction.
    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sqlx::query("commit")
        .execute(&mut running_tx)
        .await
        .unwrap();
    wait_for("initial sync of the new slot", || {
        tables_synced(&db.pool, stream_id)
    })
    .await;

    insert_event(&db.pool, stream_id, json!({"after": true})).await;
    for payload in [
        json!({"long_tx": true}),
        json!({"running_during_recovery": true}),
        json!({"while_down": true}),
        json!({"after": true}),
    ] {
        wait_for(&format!("delivery of {payload}"), || {
            delivered(&sink, payload.clone())
        })
        .await;
    }
    stop_pipeline(pipeline).await;
}

/// Events committed before the new slot existed are replayed even if they sort after the first
/// event the new slot delivers.
#[tokio::test(flavor = "multi_thread")]
async fn test_slot_recovery_replays_events_sorted_after_the_first_new_event() {
    let _lock = acquire_shared_test_lock().await;
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let stream_id = stream_config.id;
    let sink = MemorySink::new();

    let pipeline = run_until_delivered(&db, &stream_config, &sink, json!({"before": true})).await;
    record_slot_restart_lsn(&db.pool, stream_id).await.unwrap();
    stop_pipeline(pipeline).await;

    let while_down_created_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "insert into pgstream.events (payload, stream_id) values ($1, $2) returning created_at",
    )
    .bind(json!({"while_down": true}))
    .bind(stream_id as i64)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    drop_slot(&db.pool, stream_id).await;
    recover_lost_slot(&db.pool, stream_id)
        .await
        .expect("Recovery should succeed");

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    wait_for("initial sync of the new slot", || {
        tables_synced(&db.pool, stream_id)
    })
    .await;

    // The first event of the new slot sorts before `while_down`, e.g. a backdated event.
    sqlx::query("insert into pgstream.events (payload, stream_id, created_at) values ($1, $2, $3)")
        .bind(json!({"after": true}))
        .bind(stream_id as i64)
        .bind(while_down_created_at - chrono::Duration::milliseconds(1))
        .execute(&db.pool)
        .await
        .unwrap();

    wait_for("delivery after recovery", || {
        delivered(&sink, json!({"after": true}))
    })
    .await;
    wait_for(
        "replay of the event written while the slot was missing",
        || delivered(&sink, json!({"while_down": true})),
    )
    .await;
    stop_pipeline(pipeline).await;
}
