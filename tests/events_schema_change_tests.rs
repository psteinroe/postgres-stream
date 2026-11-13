//! Integration tests for schema changes of `pgstream.events` while a stream is running.

use std::collections::HashSet;
use std::time::Duration;

use etl::error::{ErrorKind, EtlResult};
use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use postgres_stream::config::StreamConfig;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{TestDatabase, test_stream_config_with_id, unique_pipeline_id};
use postgres_stream::types::SlotName;

type TestPipeline = Pipeline<PostgresStore, PgStream<MemorySink, PostgresStore>>;

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

/// Starts a pipeline and waits until the events table is synced.
async fn start_pipeline(
    db: &TestDatabase,
    stream_config: &StreamConfig,
    sink: &MemorySink,
) -> TestPipeline {
    let state_store = PostgresStore::new(stream_config.id, db.config.clone())
        .await
        .expect("Failed to create PostgresStore");
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);
    pipeline.start().await.expect("Failed to start pipeline");

    tokio::time::timeout(Duration::from_secs(30), async {
        while !table_synced(db, stream_config.id).await {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("events table should be synced");

    pipeline
}

async fn insert_event(db: &TestDatabase, stream_config: &StreamConfig, label: &str) {
    sqlx::query("insert into pgstream.events (payload, stream_id) values ($1, $2)")
        .bind(serde_json::json!({ "label": label }))
        .bind(stream_config.id as i64)
        .execute(&db.pool)
        .await
        .unwrap();
}

async fn delivered_labels(sink: &MemorySink) -> Vec<String> {
    sink.events()
        .await
        .iter()
        .filter_map(|event| event.payload.get("label")?.as_str().map(str::to_owned))
        .collect()
}

/// Waits until the sink received every label in `labels`.
async fn wait_for_labels(sink: &MemorySink, labels: &[&str]) {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let delivered: HashSet<String> = delivered_labels(sink).await.into_iter().collect();
            if labels.iter().all(|label| delivered.contains(*label)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "expected {labels:?} to be delivered, got {:?}",
        delivered_labels(sink).await
    );
}

/// Runs `ddl` the way a pgstream migration must: in one transaction, followed by
/// `pgstream.emit_events_schema_change()`.
async fn alter_events_announced(db: &TestDatabase, ddl: &str) {
    let mut tx = db.pool.begin().await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(ddl.to_owned()))
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("select pgstream.emit_events_schema_change()")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn slot_exists(db: &TestDatabase, stream_config: &StreamConfig) -> bool {
    sqlx::query_scalar("select exists (select 1 from pg_replication_slots where slot_name = $1)")
        .bind(stream_config.id.slot_name())
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

async fn shutdown(pipeline: TestPipeline) -> EtlResult<()> {
    pipeline.shutdown();
    pipeline.wait().await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_announced_events_schema_changes_are_replicated() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;

    insert_event(&db, &stream_config, "before").await;
    wait_for_labels(&sink, &["before"]).await;

    alter_events_announced(&db, "alter table pgstream.events add column extra text").await;
    sqlx::query("insert into pgstream.events (payload, stream_id, extra) values ($1, $2, 'x')")
        .bind(serde_json::json!({ "label": "after_add" }))
        .bind(stream_config.id as i64)
        .execute(&db.pool)
        .await
        .unwrap();
    wait_for_labels(&sink, &["after_add"]).await;

    alter_events_announced(&db, "alter table pgstream.events drop column extra").await;
    insert_event(&db, &stream_config, "after_drop").await;
    wait_for_labels(&sink, &["after_drop"]).await;

    shutdown(pipeline)
        .await
        .expect("pipeline should not fail on announced schema changes");

    // A pgstream upgrade migrates the events table while the pipeline is down.
    insert_event(&db, &stream_config, "down_before_alter").await;
    alter_events_announced(&db, "alter table pgstream.events add column extra2 int").await;
    insert_event(&db, &stream_config, "down_after_alter").await;

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    insert_event(&db, &stream_config, "after_restart").await;
    wait_for_labels(
        &sink,
        &["down_before_alter", "down_after_alter", "after_restart"],
    )
    .await;
    shutdown(pipeline)
        .await
        .expect("pipeline should not fail on announced schema changes");

    // Announced changes need no recovery: every event is delivered exactly once.
    let labels = delivered_labels(&sink).await;
    assert_eq!(
        labels,
        [
            "before",
            "after_add",
            "after_drop",
            "down_before_alter",
            "down_after_alter",
            "after_restart"
        ]
    );
}

/// A schema change made without `pgstream.emit_events_schema_change()` is not recovered
/// automatically: the pipeline fails loudly and leaves the slot in place. The manual recovery
/// documented in `docs/concepts/how-it-works.md` replays the missed events.
#[tokio::test(flavor = "multi_thread")]
async fn test_unannounced_events_schema_change_fails_the_pipeline() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;

    insert_event(&db, &stream_config, "before").await;
    wait_for_labels(&sink, &["before"]).await;

    // No `pgstream.emit_events_schema_change()`: etl's stored schema is stale.
    sqlx::query("alter table pgstream.events add column extra text")
        .execute(&db.pool)
        .await
        .unwrap();
    insert_event(&db, &stream_config, "after_alter").await;

    let err = tokio::time::timeout(Duration::from_secs(30), pipeline.wait())
        .await
        .expect("pipeline should fail on the unannounced schema change")
        .expect_err("pipeline should fail on the unannounced schema change");
    assert!(
        err.kinds().contains(&ErrorKind::CorruptedTableSchema),
        "unexpected error: {err}"
    );

    assert!(slot_exists(&db, &stream_config).await);
    assert_eq!(delivered_labels(&sink).await, ["before"]);

    // Manual recovery, as documented.
    let id = stream_config.id as i64;
    let slot_name = stream_config.id.slot_name();
    let mut tx = db.pool.begin().await.unwrap();
    sqlx::query(
        "update pgstream.streams
         set slot_recovery_lsn = (
           select restart_lsn - 16777216 from pg_replication_slots where slot_name = $1
         )
         where id = $2",
    )
    .bind(&slot_name)
    .bind(id)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("delete from etl.replication_state where pipeline_id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("select pg_drop_replication_slot($1)")
        .bind(&slot_name)
        .execute(&db.pool)
        .await
        .unwrap();

    let pipeline = start_pipeline(&db, &stream_config, &sink).await;
    insert_event(&db, &stream_config, "after_recovery").await;
    wait_for_labels(&sink, &["after_alter", "after_recovery"]).await;
    shutdown(pipeline)
        .await
        .expect("recovered pipeline should keep running");
}
