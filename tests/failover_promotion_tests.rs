//! Integration tests for promoting a physical standby, with and without the synced failover slot.
//!
//! Every test starts its own primary and standby, so these tests do not use the shared test
//! container. Failover slot synchronization requires Postgres 17+; on older versions the tests
//! are skipped. Pipelines start like in `core.rs`: lost slots are recovered first, and the
//! [`SlotGuardStore`] keeps etl from silently replacing a lost slot.

use std::collections::HashSet;
use std::time::Duration;

use etl::config::IntoConnectOptions;
use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use postgres_stream::config::{BatchConfig, StreamConfig};
use postgres_stream::core::recover_lost_slot;
use postgres_stream::queries::get_synchronized_standby_slots;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::slot_guard::SlotGuardStore;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{STANDBY_SLOT_NAME, StandbyPair, unique_pipeline_id};
use postgres_stream::types::SlotName;
use sqlx::PgPool;

type Store = SlotGuardStore<PostgresStore>;
type TestPipeline = Pipeline<Store, PgStream<MemorySink, Store>>;

fn failover_stream_config(pair: &StandbyPair) -> StreamConfig {
    StreamConfig {
        id: unique_pipeline_id(),
        pg_connection: pair.primary_config(),
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 200,
        },
        failover: true,
    }
}

/// Recovers a lost slot, starts a pipeline and waits until the events table is synced, so that
/// events written from then on are streamed.
async fn start_pipeline(stream_config: &StreamConfig, sink: &MemorySink) -> TestPipeline {
    let pool = PgPool::connect_with(stream_config.pg_connection.with_db(None))
        .await
        .expect("Failed to connect to database");
    recover_lost_slot(&pool, stream_config.id)
        .await
        .expect("Slot recovery should succeed");

    let state_store = SlotGuardStore::new(
        PostgresStore::new(stream_config.id, stream_config.pg_connection.clone())
            .await
            .expect("Failed to create PostgresStore"),
    );
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);
    pipeline.start().await.expect("Failed to start pipeline");

    let synced = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let states: Vec<String> = sqlx::query_scalar(
                "select state::text from etl.replication_state \
                 where pipeline_id = $1 and is_current",
            )
            .bind(stream_config.id as i64)
            .fetch_all(&pool)
            .await
            .expect("Failed to read replication state");
            if !states.is_empty()
                && states
                    .iter()
                    .all(|state| state == "sync_done" || state == "ready")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(synced.is_ok(), "events table should be synced");

    pipeline
}

async fn stop_pipeline(pipeline: TestPipeline) {
    pipeline.shutdown();
    pipeline.wait().await.expect("Pipeline should shut down");
}

/// Inserts `count` events and returns their ids.
async fn insert_events(
    pool: &PgPool,
    stream_config: &StreamConfig,
    phase: &str,
    count: usize,
) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for seq in 0..count {
        let id: String = sqlx::query_scalar(
            "insert into pgstream.events (payload, stream_id) values ($1, $2) returning id::text",
        )
        .bind(serde_json::json!({ "phase": phase, "seq": seq }))
        .bind(stream_config.id as i64)
        .fetch_one(pool)
        .await
        .expect("Failed to insert event");
        ids.push(id);
    }
    ids
}

/// Waits until the sink received every event in `expected`. Duplicates are allowed.
async fn wait_for_events(sink: &MemorySink, expected: &[String]) {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let delivered: HashSet<String> = sink
                .events()
                .await
                .into_iter()
                .map(|event| event.id.id)
                .collect();
            if expected.iter().all(|id| delivered.contains(id)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    if result.is_err() {
        let delivered: HashSet<String> = sink
            .events()
            .await
            .into_iter()
            .map(|event| event.id.id)
            .collect();
        let missing: Vec<&String> = expected
            .iter()
            .filter(|id| !delivered.contains(*id))
            .collect();
        panic!(
            "{} of {} events were not delivered: {missing:?}",
            missing.len(),
            expected.len()
        );
    }
}

/// Returns `(failover, synced)` of a replication slot, or `None` if it does not exist.
async fn slot_flags(pool: &PgPool, slot_name: &str) -> Option<(bool, bool)> {
    sqlx::query_as("select failover, synced from pg_replication_slots where slot_name = $1")
        .bind(slot_name)
        .fetch_optional(pool)
        .await
        .expect("Failed to read replication slot")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_slot_survives_switchover() {
    if !StandbyPair::is_supported() {
        eprintln!("skipping: failover slot synchronization requires Postgres 17+");
        return;
    }

    let pair = StandbyPair::start(true).await;
    let primary = pair.primary_pool().await;
    let mut stream_config = failover_stream_config(&pair);
    let slot_name = stream_config.id.slot_name();
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&stream_config, &sink).await;
    let mut expected = insert_events(&primary, &stream_config, "before", 5).await;
    wait_for_events(&sink, &expected).await;
    pair.wait_for_slot_synced(&slot_name).await;
    stop_pipeline(pipeline).await;

    expected.extend(insert_events(&primary, &stream_config, "while_down", 5).await);
    pair.promote_standby().await;

    let promoted = pair.standby_pool().await;
    stream_config.pg_connection = pair.standby_config();
    let pipeline = start_pipeline(&stream_config, &sink).await;
    expected.extend(insert_events(&promoted, &stream_config, "after_promotion", 5).await);
    wait_for_events(&sink, &expected).await;
    stop_pipeline(pipeline).await;

    // `synced` stays true on a promoted standby, so the stream reused the synced slot instead of
    // creating a new one.
    assert_eq!(slot_flags(&promoted, &slot_name).await, Some((true, true)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_events_written_right_before_failover_are_delivered() {
    if !StandbyPair::is_supported() {
        eprintln!("skipping: failover slot synchronization requires Postgres 17+");
        return;
    }

    let pair = StandbyPair::start(true).await;
    let primary = pair.primary_pool().await;
    let mut stream_config = failover_stream_config(&pair);
    let slot_name = stream_config.id.slot_name();
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&stream_config, &sink).await;
    let mut expected = insert_events(&primary, &stream_config, "before", 5).await;
    wait_for_events(&sink, &expected).await;
    pair.wait_for_slot_synced(&slot_name).await;

    // Written while the stream is running, without waiting for delivery, then the primary goes
    // away underneath the stream.
    expected.extend(insert_events(&primary, &stream_config, "right_before", 50).await);
    pair.promote_standby().await;
    pipeline.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(60), pipeline.wait())
        .await
        .expect("Pipeline should stop after losing the primary");

    let promoted = pair.standby_pool().await;
    stream_config.pg_connection = pair.standby_config();
    let pipeline = start_pipeline(&stream_config, &sink).await;
    expected.extend(insert_events(&promoted, &stream_config, "after_promotion", 5).await);
    wait_for_events(&sink, &expected).await;
    stop_pipeline(pipeline).await;

    assert_eq!(slot_flags(&promoted, &slot_name).await, Some((true, true)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_synchronized_standby_slots_lists_the_standby() {
    if !StandbyPair::is_supported() {
        eprintln!("skipping: failover slot synchronization requires Postgres 17+");
        return;
    }

    let pair = StandbyPair::start(true).await;
    let primary = pair.primary_pool().await;

    assert_eq!(
        get_synchronized_standby_slots(&primary)
            .await
            .expect("Failed to read synchronized_standby_slots"),
        Some(STANDBY_SLOT_NAME.to_owned())
    );
}

/// Without slot synchronization the slot does not exist on the promoted standby, and the events
/// written between the last confirmed position and the promotion must be replayed from the
/// events table.
#[tokio::test(flavor = "multi_thread")]
async fn test_unsynced_slot_missing_after_switchover_loses_no_events() {
    if !StandbyPair::is_supported() {
        eprintln!("skipping: failover slot synchronization requires Postgres 17+");
        return;
    }

    let pair = StandbyPair::start(false).await;
    let primary = pair.primary_pool().await;
    let mut stream_config = failover_stream_config(&pair);
    let sink = MemorySink::new();

    let pipeline = start_pipeline(&stream_config, &sink).await;
    let mut expected = insert_events(&primary, &stream_config, "before", 5).await;
    wait_for_events(&sink, &expected).await;
    stop_pipeline(pipeline).await;

    expected.extend(insert_events(&primary, &stream_config, "while_down", 5).await);
    pair.promote_standby().await;

    let promoted = pair.standby_pool().await;
    stream_config.pg_connection = pair.standby_config();
    let pipeline = start_pipeline(&stream_config, &sink).await;
    expected.extend(insert_events(&promoted, &stream_config, "after_promotion", 5).await);
    wait_for_events(&sink, &expected).await;
    stop_pipeline(pipeline).await;
}
