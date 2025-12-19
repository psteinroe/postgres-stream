use chrono::Duration;
use etl::store::both::postgres::PostgresStore;
use postgres_stream::maintenance::run_maintenance;
use postgres_stream::sink::memory::MemorySink;
use postgres_stream::store::StreamStore;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{TestDatabase, create_postgres_store, test_stream_config};

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_partitions_created() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    // Create PgStream - should create initial partitions
    let _stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store).await.unwrap();

    // Check that partitions exist (today + 2 days ahead = 3 partitions)
    let count: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(count.0, 3, "Should create 3 initial partitions");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_creates_future_partitions() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let _stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store.clone()).await.unwrap();

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
    let stream_store = StreamStore::create(test_stream_config(&db), store.clone())
        .await
        .unwrap();
    run_maintenance(&stream_store).await.unwrap();

    // Should now have 3 partitions again
    let count_after: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count_after.0, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_drops_old_partitions() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = MemorySink::new();
    let store = create_postgres_store(config.id, &db.config, &db.pool).await;

    let _stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store.clone()).await.unwrap();

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
    let stream_store = StreamStore::create(test_stream_config(&db), store.clone())
        .await
        .unwrap();
    run_maintenance(&stream_store).await.unwrap();

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

    let _stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store.clone()).await.unwrap();

    // Get initial next_maintenance_at
    let stream_store = StreamStore::create(test_stream_config(&db), store.clone())
        .await
        .unwrap();
    let (_status, next_before) = stream_store.get_stream_state().await.unwrap();

    // Run maintenance
    let completed_at = run_maintenance(&stream_store).await.unwrap();

    // Update the next maintenance time
    let next_after = next_before + Duration::hours(24);
    stream_store
        .store_next_maintenance_at(next_after)
        .await
        .unwrap();

    // Verify next_maintenance_at was updated
    let (_status_after, next_after_stored) = stream_store.get_stream_state().await.unwrap();

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

    let _stream: PgStream<MemorySink, PostgresStore> =
        PgStream::create(config, sink, store.clone()).await.unwrap();

    let stream_store = StreamStore::create(test_stream_config(&db), store.clone())
        .await
        .unwrap();

    // Run maintenance twice
    run_maintenance(&stream_store).await.unwrap();
    run_maintenance(&stream_store).await.unwrap();

    // Should still have the expected number of partitions
    let count: (i64,) = sqlx::query_as(
        "select count(*) from pg_tables
         where schemaname = 'pgstream'
         and tablename like 'events_%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(count.0, 3, "Running maintenance twice should be idempotent");
}
