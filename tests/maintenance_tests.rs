use chrono::Duration;
use etl::store::both::postgres::PostgresStore;
use postgres_stream::maintenance::run_maintenance;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{TestDatabase, create_postgres_store, test_stream_config};

async fn assert_events_lsn_brin_indexes(db: &TestDatabase) {
    let (parent_index_valid,): (bool,) = sqlx::query_as(
        "select exists(
            select 1
            from pg_class as index_relation
            join pg_namespace as index_namespace
              on index_namespace.oid = index_relation.relnamespace
            join pg_am as access_method
              on access_method.oid = index_relation.relam
            join pg_index as index_metadata
              on index_metadata.indexrelid = index_relation.oid
            where index_namespace.nspname = 'pgstream'
              and index_relation.relname = 'events_lsn_brin_idx'
              and index_relation.relkind = 'I'
              and access_method.amname = 'brin'
              and index_metadata.indisready
              and index_metadata.indisvalid
        )",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(parent_index_valid, "parent BRIN index should be valid");

    let (partition_count, attached_index_count): (i64, i64) = sqlx::query_as(
        "with leaf_partitions as (
            select partition_tree.relid, partition_relation.relname
            from pg_partition_tree('pgstream.events'::regclass) as partition_tree
            join pg_class as partition_relation
              on partition_relation.oid = partition_tree.relid
            where partition_tree.isleaf
        ), parent_index as (
            select index_relation.oid
            from pg_class as index_relation
            join pg_namespace as index_namespace
              on index_namespace.oid = index_relation.relnamespace
            where index_namespace.nspname = 'pgstream'
              and index_relation.relname = 'events_lsn_brin_idx'
        )
        select
            (select count(*) from leaf_partitions),
            (select count(*)
             from leaf_partitions
             where exists(
                 select 1
                 from pg_index as child_index
                 join pg_class as child_index_relation
                   on child_index_relation.oid = child_index.indexrelid
                 join pg_am as access_method
                   on access_method.oid = child_index_relation.relam
                 join pg_inherits as index_attachment
                   on index_attachment.inhrelid = child_index.indexrelid
                 where child_index.indrelid = leaf_partitions.relid
                   and child_index.indnatts = 1
                   and child_index.indkey[0] = (
                       select attnum
                       from pg_attribute
                       where attrelid = leaf_partitions.relid
                         and attname = 'lsn'
                         and not attisdropped
                   )
                   and access_method.amname = 'brin'
                   and child_index.indisready
                   and child_index.indisvalid
                   and index_attachment.inhparent = (select oid from parent_index)
             ))",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(
        attached_index_count, partition_count,
        "every events leaf partition should have a valid attached BRIN index"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_partitions_created() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    // Create PgStream - should create initial partitions
    let _stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Check that partitions exist (today + 6 days ahead = 7 partitions)
    let count: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(count.0, 7, "Should create 7 initial partitions");
    assert_events_lsn_brin_indexes(&db).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_creates_future_partitions() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Manually delete all partitions except today
    sqlx::query(
        "do $$
        declare
            partition_name text;
        begin
            for partition_name in
                select tablename from pg_tables
                where schemaname = 'pgstream'
                and tablename like 'events_%'
                and tablename != concat('events_', to_char(now(), 'YYYYMMDD'))
            loop
                execute 'drop table if exists pgstream.' || partition_name;
            end loop;
        end $$;",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    // Verify only 1 partition exists
    let count_before: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count_before.0, 1);

    // Run maintenance manually
    let scheduled_time = chrono::Utc::now();
    run_maintenance(stream.store(), scheduled_time)
        .await
        .unwrap();

    // Should now have 7 partitions again (today + 6 days ahead)
    let count_after: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count_after.0, 7);
    assert_events_lsn_brin_indexes(&db).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_drops_old_partitions() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Create an old partition (10 days ago - beyond 7 day retention)
    let old_date = chrono::Utc::now() - Duration::days(10);
    let old_partition_name = format!("events_{}", old_date.format("%Y%m%d"));
    sqlx::query(&format!(
        "create table pgstream.{} partition of pgstream.events
         for values from ('{}') to ('{}')",
        old_partition_name,
        old_date.format("%Y-%m-%d"),
        (old_date + Duration::days(1)).format("%Y-%m-%d")
    ))
    .execute(&db.pool)
    .await
    .unwrap();

    // Verify old partition exists
    let exists_before: (bool,) = sqlx::query_as(&format!(
        "select exists(
            select 1 from pg_tables
            where schemaname = 'pgstream'
            and tablename = '{old_partition_name}'
        )"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(exists_before.0);

    // Run maintenance
    let scheduled_time = chrono::Utc::now();
    run_maintenance(stream.store(), scheduled_time)
        .await
        .unwrap();

    // Old partition should be dropped
    let exists_after: (bool,) = sqlx::query_as(&format!(
        "select exists(
            select 1 from pg_tables
            where schemaname = 'pgstream'
            and tablename = '{old_partition_name}'
        )"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(!exists_after.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_updates_next_maintenance_at() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Get initial next_maintenance_at
    let (_status, next_before) = stream.store().get_stream_state().await.unwrap();

    // Run maintenance
    let scheduled_time = next_before;
    let completed_at = run_maintenance(stream.store(), scheduled_time)
        .await
        .unwrap();

    // Update the next maintenance time
    let next_after = next_before + Duration::hours(24);
    stream
        .store()
        .store_next_maintenance_at(next_after)
        .await
        .unwrap();

    // Verify next_maintenance_at was updated
    let (_status_after, next_after_stored) = stream.store().get_stream_state().await.unwrap();

    assert!(
        next_after_stored > next_before,
        "next_maintenance_at should be updated"
    );
    assert!(
        completed_at <= next_after_stored,
        "next run should be scheduled after completion"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_idempotent() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Run maintenance twice
    let scheduled_time = chrono::Utc::now();
    run_maintenance(stream.store(), scheduled_time)
        .await
        .unwrap();
    run_maintenance(stream.store(), scheduled_time)
        .await
        .unwrap();

    // Should still have the expected number of partitions
    let count: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(count.0, 7, "Running maintenance twice should be idempotent");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_run_maintenance_returns_scheduled_time() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Use a specific scheduled_time (1 hour in the past)
    let scheduled_time = chrono::Utc::now() - Duration::hours(1);
    let returned_time = run_maintenance(stream.store(), scheduled_time)
        .await
        .unwrap();

    // run_maintenance should return the scheduled_time, not the execution time
    assert_eq!(
        returned_time, scheduled_time,
        "run_maintenance should return the scheduled_time passed to it"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_scheduling_uses_scheduled_time_not_execution_time() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Simulate a maintenance that was scheduled for 2 hours ago
    // This can happen if the system was down during the scheduled maintenance time
    let original_scheduled_time = chrono::Utc::now() - Duration::hours(2);

    // Run maintenance with the original scheduled time
    let returned_time = run_maintenance(stream.store(), original_scheduled_time)
        .await
        .unwrap();

    // The next maintenance should be scheduled 24h from the ORIGINAL scheduled time,
    // not 24h from now. This ensures consistent daily scheduling even if
    // maintenance runs late.
    let expected_next_maintenance = original_scheduled_time + Duration::hours(24);
    stream
        .store()
        .store_next_maintenance_at(expected_next_maintenance)
        .await
        .unwrap();

    let (_status, actual_next_maintenance) = stream.store().get_stream_state().await.unwrap();

    // Verify the next maintenance is scheduled based on the original scheduled time
    assert_eq!(
        actual_next_maintenance, expected_next_maintenance,
        "Next maintenance should be 24h from original scheduled time, not from execution time"
    );

    // Also verify that the return value is the scheduled time
    assert_eq!(
        returned_time, original_scheduled_time,
        "run_maintenance should return the scheduled_time for scheduling purposes"
    );
}
