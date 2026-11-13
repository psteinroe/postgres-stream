//! Integration tests for a running pipeline's lifecycle: shutdown with a slow sink, concurrent
//! instances of one stream, partitions created while running and idle streams.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use etl::config::PipelineConfig;
use etl::error::{ErrorKind, EtlResult};
use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use postgres_stream::config::StreamConfig;
use postgres_stream::maintenance::PartitionInfo;
use postgres_stream::queries::create_partition;
use postgres_stream::sink::Sink;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{TestDatabase, test_stream_config_with_id, unique_pipeline_id};
use postgres_stream::types::{SlotName, TriggeredEvent};
use tokio::sync::Notify;

type TestPipeline<S> = Pipeline<PostgresStore, PgStream<S, PostgresStore>>;

/// A sink whose publishes never complete while it is blocked.
#[derive(Clone, Default)]
struct BlockingSink {
    inner: MemorySink,
    blocked: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
    publish_started: Arc<Notify>,
}

impl BlockingSink {
    fn blocked() -> Self {
        let sink = Self::default();
        sink.blocked.store(true, Ordering::SeqCst);
        sink
    }
}

impl Sink for BlockingSink {
    fn name() -> &'static str {
        "blocking_sink"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if self.blocked.load(Ordering::SeqCst) {
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            self.publish_started.notify_one();
            std::future::pending::<()>().await;
        }

        self.inner.publish_events(events).await
    }
}

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

async fn wait_until_synced(db: &TestDatabase, pipeline_id: u64) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !table_synced(db, pipeline_id).await {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("events table should be synced");
}

/// Creates a pipeline for `stream_config` without starting it.
async fn create_pipeline<S>(
    db: &TestDatabase,
    stream_config: &StreamConfig,
    sink: S,
    configure: impl FnOnce(&mut PipelineConfig),
) -> TestPipeline<S>
where
    S: Sink + Clone + Send + Sync + 'static,
{
    let state_store = PostgresStore::new(stream_config.id, db.config.clone())
        .await
        .expect("Failed to create PostgresStore");
    let pgstream = PgStream::create(stream_config.clone(), sink, state_store.clone())
        .await
        .expect("Failed to create PgStream");
    let mut pipeline_config: PipelineConfig = stream_config.clone().into();
    configure(&mut pipeline_config);

    Pipeline::new(pipeline_config, state_store, pgstream)
}

/// Starts a pipeline and waits until the events table is synced.
async fn start_pipeline<S>(
    db: &TestDatabase,
    stream_config: &StreamConfig,
    sink: S,
    configure: impl FnOnce(&mut PipelineConfig),
) -> TestPipeline<S>
where
    S: Sink + Clone + Send + Sync + 'static,
{
    let mut pipeline = create_pipeline(db, stream_config, sink, configure).await;
    pipeline.start().await.expect("Failed to start pipeline");
    wait_until_synced(db, stream_config.id).await;

    pipeline
}

async fn shutdown<S>(pipeline: TestPipeline<S>) -> EtlResult<()>
where
    S: Sink + Clone + Send + Sync + 'static,
{
    pipeline.shutdown();
    pipeline.wait().await
}

/// Inserts an event labelled `label` and returns its `lsn`.
async fn insert_event(db: &TestDatabase, stream_config: &StreamConfig, label: &str) -> String {
    sqlx::query_scalar(
        "insert into pgstream.events (payload, stream_id) values ($1, $2) returning lsn::text",
    )
    .bind(serde_json::json!({ "label": label }))
    .bind(stream_config.id as i64)
    .fetch_one(&db.pool)
    .await
    .unwrap()
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

async fn confirmed_flush_lsn(db: &TestDatabase, stream_config: &StreamConfig) -> Option<String> {
    sqlx::query_scalar(
        "select confirmed_flush_lsn::text from pg_replication_slots where slot_name = $1",
    )
    .bind(stream_config.id.slot_name())
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .flatten()
}

/// Waits until the slot's `confirmed_flush_lsn` is at least `lsn`.
async fn wait_for_confirmed_flush(db: &TestDatabase, stream_config: &StreamConfig, lsn: &str) {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let reached: bool = sqlx::query_scalar(
                "select coalesce(confirmed_flush_lsn >= $2::pg_lsn, false)
                 from pg_replication_slots where slot_name = $1",
            )
            .bind(stream_config.id.slot_name())
            .bind(lsn)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            if reached {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "confirmed_flush_lsn should reach {lsn}, is {:?}",
        confirmed_flush_lsn(db, stream_config).await
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_with_blocked_publish_redelivers_after_restart() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());

    let blocking_sink = BlockingSink::blocked();
    let pipeline = start_pipeline(&db, &stream_config, blocking_sink.clone(), |config| {
        config.shutdown_drain_timeout_ms = 1_000;
    })
    .await;

    let first_lsn = insert_event(&db, &stream_config, "a").await;
    for label in ["b", "c"] {
        insert_event(&db, &stream_config, label).await;
    }

    tokio::time::timeout(
        Duration::from_secs(30),
        blocking_sink.publish_started.notified(),
    )
    .await
    .expect("publish should be in flight");

    // The drain deadline cancels the stuck publish instead of hanging shutdown.
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(20), shutdown(pipeline))
        .await
        .expect("shutdown should finish after the drain timeout");
    assert!(result.is_ok(), "forced shutdown should succeed: {result:?}");
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "shutdown should wait for the drain timeout"
    );
    assert!(blocking_sink.inner.events().await.is_empty());

    // The blocked events were never confirmed, so the slot streams them again.
    let confirmed: bool = sqlx::query_scalar(
        "select confirmed_flush_lsn > $2::pg_lsn from pg_replication_slots where slot_name = $1",
    )
    .bind(stream_config.id.slot_name())
    .bind(&first_lsn)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(
        !confirmed,
        "the slot must not be confirmed past the blocked event at {first_lsn}, is at {:?}",
        confirmed_flush_lsn(&db, &stream_config).await
    );

    let sink = MemorySink::new();
    let pipeline = start_pipeline(&db, &stream_config, sink.clone(), |_| {}).await;
    insert_event(&db, &stream_config, "after_restart").await;
    wait_for_labels(&sink, &["a", "b", "c", "after_restart"]).await;
    shutdown(pipeline).await.expect("pipeline should shut down");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_second_instance_of_a_stream_does_not_disturb_the_first() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());

    let first_sink = MemorySink::new();
    let first = start_pipeline(&db, &stream_config, first_sink.clone(), |_| {}).await;
    insert_event(&db, &stream_config, "before_second").await;
    wait_for_labels(&first_sink, &["before_second"]).await;

    // A rolling deploy starts a second instance while the first still streams.
    let second_sink = MemorySink::new();
    let mut second = create_pipeline(&db, &stream_config, second_sink.clone(), |_| {}).await;
    let second_result = match second.start().await {
        Err(err) => Err(err),
        Ok(()) => tokio::time::timeout(Duration::from_secs(30), second.wait())
            .await
            .expect("second instance should fail instead of running"),
    };
    // The slot is in use: the second instance fails permanently, so the daemon exits and the
    // orchestrator restarts it with backoff until the first instance releases the slot.
    let err = second_result.expect_err("the slot is in use by the first instance");
    assert!(
        err.kinds().contains(&ErrorKind::InvalidState) && err.to_string().contains("active"),
        "unexpected error: {err}"
    );

    insert_event(&db, &stream_config, "after_second").await;
    wait_for_labels(&first_sink, &["before_second", "after_second"]).await;
    assert!(second_sink.events().await.is_empty());
    shutdown(first)
        .await
        .expect("first instance should keep running");

    // The stream state is intact: a new instance picks up where the first stopped.
    insert_event(&db, &stream_config, "while_down").await;
    let third_sink = MemorySink::new();
    let third = start_pipeline(&db, &stream_config, third_sink.clone(), |_| {}).await;
    insert_event(&db, &stream_config, "after_restart").await;
    wait_for_labels(&third_sink, &["while_down", "after_restart"]).await;
    shutdown(third).await.expect("pipeline should shut down");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partition_created_while_running_is_replicated() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&db, &stream_config, sink.clone(), |_| {}).await;
    insert_event(&db, &stream_config, "existing_partition").await;
    wait_for_labels(&sink, &["existing_partition"]).await;

    // Beyond the partitions startup maintenance created.
    let date = Utc::now().date_naive() + chrono::Duration::days(30);
    let partition = PartitionInfo::for_date("events", date);
    let (start, end) = partition.range_bounds();
    create_partition(
        &db.pool,
        "pgstream",
        "events",
        &partition.name,
        &start,
        &end,
    )
    .await
    .unwrap();

    sqlx::query("insert into pgstream.events (payload, stream_id, created_at) values ($1, $2, $3)")
        .bind(serde_json::json!({ "label": "new_partition" }))
        .bind(stream_config.id as i64)
        .bind(date.and_hms_opt(12, 0, 0).unwrap().and_utc())
        .execute(&db.pool)
        .await
        .unwrap();
    wait_for_labels(&sink, &["existing_partition", "new_partition"]).await;

    shutdown(pipeline).await.expect("pipeline should shut down");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_idle_stream_delivers_later_events_and_advances_slot() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;
    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&db, &stream_config, sink.clone(), |_| {}).await;
    let first_lsn = insert_event(&db, &stream_config, "first").await;
    wait_for_labels(&sink, &["first"]).await;
    wait_for_confirmed_flush(&db, &stream_config, &first_lsn).await;

    // No events for a while, only unrelated WAL: etl sees keepalives and empty transactions.
    sqlx::query("create table unrelated (id int)")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut unrelated_lsn = String::new();
    for i in 0..20 {
        unrelated_lsn = sqlx::query_scalar(
            "with inserted as (insert into unrelated values ($1)) select pg_current_wal_lsn()::text",
        )
        .bind(i)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The slot advances past WAL without events.
    wait_for_confirmed_flush(&db, &stream_config, &unrelated_lsn).await;

    let later_lsn = insert_event(&db, &stream_config, "later").await;
    wait_for_labels(&sink, &["first", "later"]).await;
    wait_for_confirmed_flush(&db, &stream_config, &later_lsn).await;
    assert_eq!(delivered_labels(&sink).await, ["first", "later"]);

    shutdown(pipeline).await.expect("pipeline should shut down");
}
