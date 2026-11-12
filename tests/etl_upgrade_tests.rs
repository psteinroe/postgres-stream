//! Upgrade tests for databases that an earlier pgstream (etl `3122cb1`) replicated from.
//!
//! That version applied etl's base migration with its own SQLx runner (recording it in
//! `etl._sqlx_migrations`) and left the events table's replication state, stored schema,
//! publication, and a plain logical slot behind. The rows below mirror what it wrote.

use std::borrow::Cow;
use std::time::Duration;

use etl::config::IntoConnectOptions;
use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use postgres_stream::migrations::migrate_etl;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    TestDatabase, slot_failover, supports_failover_slots, test_stream_config_with_id,
    unique_pipeline_id,
};
use postgres_stream::types::SlotName;
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, PgPool, migrate::Migrator};

/// etl's base migration exactly as pgstream vendored it in `etl-migrations/`.
static OLD_ETL_MIGRATOR: Migrator = sqlx::migrate!("./tests/fixtures/old_etl_migrations");
static PGSTREAM_MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// First pgstream migration that is not part of the old release.
const ETL_SCHEMA_HELPERS_VERSION: i64 = 1_794_493_794_585;

/// Checksum the old pgstream recorded for etl's base migration.
const OLD_BASE_CHECKSUM: &str = "f9a851cc59b9777ef680c271ba52f060da848b31fa6cd77123b6205f9dcd99ff332981954dcadda62a784ce7aed1e881";

/// etl's state store migrations (`crates/etl/migrations/postgres_store`) as of `d5098c27`.
const ETL_STORE_VERSIONS: [i64; 9] = [
    20250827000000,
    20260415090000,
    20260511090000,
    20260610120000,
    20260803120000,
    20260810120000,
    20260813120000,
    20260813120100,
    20261002120100,
];

/// etl's source migrations, which pgstream does not run.
const ETL_SOURCE_VERSIONS: [i64; 3] = [20260415100000, 20260724120000, 20261002120000];

/// Applies the migrations the old pgstream ran: etl's base migration and pgstream's own
/// migrations up to that release.
async fn apply_old_migrations(db: &TestDatabase) {
    let mut conn = PgConnection::connect_with(&db.config.with_db(None))
        .await
        .unwrap();

    conn.execute("create schema if not exists etl; set search_path = 'etl';")
        .await
        .unwrap();
    OLD_ETL_MIGRATOR
        .run_direct(None, &mut conn, false)
        .await
        .expect("Failed to apply the old etl base migration");

    conn.execute(
        r#"create extension if not exists "uuid-ossp";
           create schema if not exists pgstream;
           set search_path = 'pgstream';"#,
    )
    .await
    .unwrap();
    let old_pgstream_migrator = Migrator {
        migrations: Cow::Owned(
            PGSTREAM_MIGRATOR
                .iter()
                .filter(|migration| migration.version < ETL_SCHEMA_HELPERS_VERSION)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    old_pgstream_migrator
        .run_direct(None, &mut conn, false)
        .await
        .expect("Failed to apply the old pgstream migrations");
}

/// Writes the state an old pipeline leaves behind once the events table is ready.
async fn insert_old_pipeline_state(pool: &PgPool, pipeline_id: u64) {
    // The streams row creates the stream's publication.
    sqlx::query("insert into pgstream.streams (id, next_maintenance_at) values ($1, now())")
        .bind(pipeline_id as i64)
        .execute(pool)
        .await
        .unwrap();

    sqlx::raw_sql(AssertSqlSafe(format!(
        r#"
        with init as (
            insert into etl.replication_state (pipeline_id, table_id, state, metadata, is_current)
            values ({pipeline_id}, 'pgstream.events'::regclass, 'init', '{{"type": "init"}}', false)
            returning id
        ), data_sync as (
            insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
            select {pipeline_id}, 'pgstream.events'::regclass, 'data_sync', '{{"type": "data_sync"}}', id, false
            from init
            returning id
        ), finished_copy as (
            insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
            select {pipeline_id}, 'pgstream.events'::regclass, 'finished_copy', '{{"type": "finished_copy"}}', id, false
            from data_sync
            returning id
        ), sync_done as (
            insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
            select {pipeline_id}, 'pgstream.events'::regclass, 'sync_done',
                   jsonb_build_object('type', 'sync_done', 'lsn', pg_current_wal_lsn()::text), id, false
            from finished_copy
            returning id
        )
        insert into etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current)
        select {pipeline_id}, 'pgstream.events'::regclass, 'ready', '{{"type": "ready"}}', id, true
        from sync_done;

        with table_schema as (
            insert into etl.table_schemas (pipeline_id, table_id, schema_name, table_name)
            values ({pipeline_id}, 'pgstream.events'::regclass, 'pgstream', 'events')
            returning id
        )
        insert into etl.table_columns
            (table_schema_id, column_name, column_type, type_modifier, nullable, primary_key, column_order)
        select table_schema.id, c.column_name, c.column_type, -1, c.nullable, c.primary_key, c.column_order
        from table_schema
        cross join (values
            ('id', 'UUID', false, true, 0),
            ('payload', 'JSONB', false, false, 1),
            ('metadata', 'JSONB', true, false, 2),
            ('stream_id', 'INT8', false, false, 3),
            ('created_at', 'TIMESTAMPTZ', false, true, 4),
            ('lsn', 'PG_LSN', true, false, 5)
        ) as c(column_name, column_type, nullable, primary_key, column_order);
        "#
    )))
    .execute(pool)
    .await
    .expect("Failed to write the old pipeline state");

    sqlx::query("select pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(pipeline_id.slot_name())
        .execute(pool)
        .await
        .expect("Failed to create the old replication slot");
}

async fn applied_migrations(pool: &PgPool, schema: &str) -> Vec<(i64, String)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "select version, encode(checksum, 'hex') from {schema}._sqlx_migrations \
         where success order by version"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn assert_source_helpers_without_event_trigger(pool: &PgPool) {
    let (has_schema_helper, has_identity_helper, has_trigger, has_trigger_function): (
        bool,
        bool,
        bool,
        bool,
    ) = sqlx::query_as(
        r#"
        select
            to_regprocedure('etl.describe_table_schema(oid)') is not null,
            to_regprocedure('etl.describe_table_identity(oid)') is not null,
            exists (select 1 from pg_event_trigger where evtname = 'supabase_etl_ddl_message_trigger'),
            to_regprocedure('etl.emit_schema_change_messages()') is not null
        "#,
    )
    .fetch_one(pool)
    .await
    .unwrap();

    assert!(has_schema_helper, "etl.describe_table_schema should exist");
    assert!(
        has_identity_helper,
        "etl.describe_table_identity should exist"
    );
    assert!(
        !has_trigger,
        "the DDL event trigger should not be installed"
    );
    assert!(
        !has_trigger_function,
        "etl.emit_schema_change_messages should not be installed"
    );

    // The deferrability-aware identity helper (etl 20261002120000) is installed.
    let identity: serde_json::Value =
        sqlx::query_scalar("select etl.describe_table_identity('pgstream.events'::regclass)")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(
        identity.get("primary_key_condeferrable"),
        Some(&serde_json::Value::Bool(false))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fresh_database_runs_etl_store_and_helper_migrations() {
    let db = TestDatabase::spawn().await;

    let etl_versions: Vec<i64> = applied_migrations(&db.pool, "etl")
        .await
        .into_iter()
        .map(|(version, _)| version)
        .collect();
    assert_eq!(etl_versions, ETL_STORE_VERSIONS);

    let pgstream_versions: Vec<i64> = applied_migrations(&db.pool, "pgstream")
        .await
        .into_iter()
        .map(|(version, _)| version)
        .collect();
    assert!(pgstream_versions.contains(&ETL_SCHEMA_HELPERS_VERSION));

    assert_source_helpers_without_event_trigger(&db.pool).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upgrade_from_old_pgstream_reuses_state_and_slot() {
    let db = TestDatabase::spawn_without_migrations().await;
    apply_old_migrations(&db).await;
    db.ensure_today_partition().await;

    let mut stream_config = test_stream_config_with_id(&db, unique_pipeline_id());
    let pipeline_id = stream_config.id;
    let slot_name = pipeline_id.slot_name();
    insert_old_pipeline_state(&db.pool, pipeline_id).await;

    let old_etl_migrations = applied_migrations(&db.pool, "etl").await;
    assert_eq!(
        old_etl_migrations,
        vec![(ETL_STORE_VERSIONS[0], OLD_BASE_CHECKSUM.to_string())]
    );

    // Written after the old pipeline stopped: only delivered if the existing slot is reused.
    sqlx::query("insert into pgstream.events (payload, stream_id) values ($1, $2)")
        .bind(serde_json::json!({"written_before_upgrade": true}))
        .bind(pipeline_id as i64)
        .execute(&db.pool)
        .await
        .unwrap();

    // Start the new version the way `core::start_pipeline_with_config` does.
    let failover = supports_failover_slots(&db.pool).await;
    stream_config.failover = failover;
    migrate_etl(&db.config)
        .await
        .expect("etl store migrations should upgrade the old database");

    let sink = MemorySink::new();
    let state_store = PostgresStore::new(pipeline_id, db.config.clone())
        .await
        .expect("Failed to create PostgresStore");
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);
    pipeline.start().await.expect("Failed to start pipeline");

    let delivered = tokio::time::timeout(Duration::from_secs(30), async {
        while sink.events().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;

    pipeline.shutdown();
    pipeline.wait().await.expect("Pipeline should shut down");

    assert!(
        delivered.is_ok(),
        "event written before the upgrade should be delivered"
    );
    let events = sink.events().await;
    assert_eq!(
        events.first().map(|event| event.payload.clone()),
        Some(serde_json::json!({"written_before_upgrade": true}))
    );

    // All etl store migrations applied on top of the old base, whose checksum still matches.
    let etl_migrations = applied_migrations(&db.pool, "etl").await;
    assert_eq!(
        etl_migrations
            .iter()
            .map(|(version, _)| *version)
            .collect::<Vec<_>>(),
        ETL_STORE_VERSIONS
    );
    assert_eq!(
        etl_migrations
            .first()
            .map(|(_, checksum)| checksum.as_str()),
        Some(OLD_BASE_CHECKSUM)
    );
    assert!(
        etl_migrations
            .iter()
            .all(|(version, _)| !ETL_SOURCE_VERSIONS.contains(version))
    );
    assert_source_helpers_without_event_trigger(&db.pool).await;

    // The old state was migrated in place instead of being reset.
    let states: Vec<String> = sqlx::query_scalar(
        "select state::text from etl.replication_state where pipeline_id = $1 and is_current",
    )
    .bind(pipeline_id as i64)
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(states, vec!["ready".to_string()]);

    let stored_schemas: i64 =
        sqlx::query_scalar("select count(*) from etl.table_schemas where pipeline_id = $1")
            .bind(pipeline_id as i64)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(stored_schemas, 1, "the old stored schema should be reused");

    if failover {
        assert_eq!(slot_failover(&db.pool, &slot_name).await, Some(true));
    } else {
        let slot_exists: bool = sqlx::query_scalar(
            "select exists (select 1 from pg_replication_slots where slot_name = $1)",
        )
        .bind(&slot_name)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(slot_exists);
    }
}
