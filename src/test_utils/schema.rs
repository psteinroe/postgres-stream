use etl::config::PgConnectionConfig;
use etl::store::both::postgres::PostgresStore;
use etl::store::schema::SchemaStore;
use etl::types::{ColumnSchema, TableId, TableName, TableSchema};
use sqlx::PgPool;
use tokio_postgres::types::Type;

/// Initialize the schema cache for the events table in PostgresStore.
///
/// This is necessary for tests that use `write_events` or `write_table_rows`,
/// as those methods require the schema to be in the cache.
pub async fn init_events_schema(
    store: &PostgresStore,
    pool: &PgPool,
) -> Result<TableId, sqlx::Error> {
    // Fetch the events table OID
    let row: (i64,) = sqlx::query_as(
        "select c.oid::bigint
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'pgstream'
         and c.relname = 'events'",
    )
    .fetch_one(pool)
    .await?;

    let table_id = TableId::new(row.0 as u32);

    // Manually create the schema for the events table
    // Column order MUST match the COPY query in replay_client.rs:
    // select id, payload, metadata, stream_id, created_at, lsn
    let schema = TableSchema {
        id: table_id,
        name: TableName::new("pgstream".to_string(), "events".to_string()),
        column_schemas: vec![
            ColumnSchema::new("id".to_string(), Type::UUID, -1, false, true),
            ColumnSchema::new("payload".to_string(), Type::JSONB, -1, true, false),
            ColumnSchema::new("metadata".to_string(), Type::JSONB, -1, true, false),
            ColumnSchema::new("stream_id".to_string(), Type::INT8, -1, false, false),
            ColumnSchema::new("created_at".to_string(), Type::TIMESTAMPTZ, -1, false, true),
            ColumnSchema::new("lsn".to_string(), Type::TEXT, -1, true, false),
        ],
    };

    store
        .store_table_schema(schema)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("Failed to store table schema: {e:?}")))?;

    Ok(table_id)
}

/// Create a PostgresStore and initialize it with the events table schema.
///
/// This is a convenience helper for tests that need a working PostgresStore.
pub async fn create_postgres_store(
    stream_id: u64,
    config: &PgConnectionConfig,
    pool: &PgPool,
) -> PostgresStore {
    let store = PostgresStore::new(stream_id, config.clone());
    init_events_schema(&store, pool)
        .await
        .expect("Failed to initialize events schema");
    store
}

/// Create a PostgresStore and initialize it with the events table schema.
///
/// Returns both the store and the events table_id (OID) for use in creating test events.
/// Use this variant when you need to create Event::Insert with the correct table_id.
pub async fn create_postgres_store_with_table_id(
    stream_id: u64,
    config: &PgConnectionConfig,
    pool: &PgPool,
) -> (PostgresStore, TableId) {
    let store = PostgresStore::new(stream_id, config.clone());
    let table_id = init_events_schema(&store, pool)
        .await
        .expect("Failed to initialize events schema");
    (store, table_id)
}
