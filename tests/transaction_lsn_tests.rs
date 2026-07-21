use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{TestDatabase, test_stream_config_with_id, unique_pipeline_id};
use std::collections::HashSet;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_and_update_share_commit_lsn_distinct_from_row_lsns() {
    let db = TestDatabase::spawn().await;
    db.ensure_today_partition().await;

    sqlx::query(
        r#"
        create table public.users (
            id serial primary key,
            name text not null
        )
        "#,
    )
    .execute(&db.pool)
    .await
    .unwrap();

    let stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let pipeline_id = stream_config.id;

    for (key, operation) in [("user_insert", "INSERT"), ("user_update", "UPDATE")] {
        sqlx::query(
            r#"
            insert into pgstream.subscriptions
                (key, stream_id, operation, schema_name, table_name, column_names, payload_extensions, metadata_extensions)
            values
                ($1, $2, $3::pgstream.operation_type, 'public', 'users', array['id', 'name'], '[]', '[]')
            "#,
        )
        .bind(key)
        .bind(pipeline_id as i64)
        .bind(operation)
        .execute(&db.pool)
        .await
        .unwrap();
    }

    let sink = MemorySink::new();
    let state_store = PostgresStore::new(pipeline_id, db.config.clone());
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .unwrap();
    let pipeline_config: etl::config::PipelineConfig = stream_config.into();
    let mut pipeline = Pipeline::new(pipeline_config, state_store, pgstream);
    pipeline.start().await.unwrap();

    let slot_name = format!("supabase_etl_apply_{pipeline_id}");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let slot_exists: bool = sqlx::query_scalar(
                "select exists(select 1 from pg_replication_slots where slot_name = $1)",
            )
            .bind(&slot_name)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            let states: Vec<String> = sqlx::query_scalar(
                "select state::text from etl.replication_state where pipeline_id = $1 and is_current = true",
            )
            .bind(pipeline_id as i64)
            .fetch_all(&db.pool)
            .await
            .unwrap_or_default();

            if slot_exists
                && !states.is_empty()
                && states
                    .iter()
                    .all(|state| state == "sync_done" || state == "ready")
            {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("pipeline should initialize");

    sqlx::query(
        "insert into pgstream.events (payload, stream_id, lsn) values ($1, $2, pg_current_wal_lsn())",
    )
    .bind(serde_json::json!({"pipeline_ready": true}))
    .bind(pipeline_id as i64)
    .execute(&db.pool)
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let states: Vec<String> = sqlx::query_scalar(
                "select state::text from etl.replication_state where pipeline_id = $1 and is_current = true",
            )
            .bind(pipeline_id as i64)
            .fetch_all(&db.pool)
            .await
            .unwrap_or_default();
            let warmup_received = sink.events().await.iter().any(|event| {
                event
                    .payload
                    .get("pipeline_ready")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
            });

            if !states.is_empty()
                && states.iter().all(|state| state == "ready")
                && warmup_received
            {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("pipeline should become ready");
    sink.clear().await;

    let mut transaction = db.pool.begin().await.unwrap();
    let user_id: i32 =
        sqlx::query_scalar("insert into public.users (name) values ('before') returning id")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    sqlx::query("update public.users set name = 'after' where id = $1")
        .bind(user_id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let transaction_events = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let events = sink.events().await;
            let transaction_events: Vec<_> = events
                .into_iter()
                .filter(|event| {
                    matches!(
                        event
                            .payload
                            .get("tg_op")
                            .and_then(serde_json::Value::as_str),
                        Some("INSERT" | "UPDATE")
                    )
                })
                .collect();

            if transaction_events.len() == 2 {
                break transaction_events;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    let shutdown_tx = pipeline.shutdown_tx();
    shutdown_tx.shutdown().unwrap();
    pipeline.wait().await.unwrap();

    let transaction_events = transaction_events.expect("transaction events should be replicated");
    let operations: HashSet<_> = transaction_events
        .iter()
        .filter_map(|event| {
            event
                .payload
                .get("tg_op")
                .and_then(serde_json::Value::as_str)
        })
        .collect();
    assert_eq!(operations, HashSet::from(["INSERT", "UPDATE"]));

    let commit_lsn = transaction_events
        .first()
        .and_then(|event| event.commit_lsn)
        .expect("live events should have a commit LSN");

    assert!(transaction_events.iter().all(|event| {
        event.commit_lsn == Some(commit_lsn) && event.lsn.is_some() && event.lsn != Some(commit_lsn)
    }));
}
