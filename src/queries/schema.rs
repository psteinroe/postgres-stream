use etl::{
    error::EtlResult,
    types::{ColumnSchema, TableId, TableName, TableSchema, convert_type_oid_to_type},
};
use sqlx::PgPool;
use sqlx::postgres::types::Oid as SqlxTableId;

use crate::config::{EVENTS_TABLE, SCHEMA_NAME};

pub async fn fetch_events_table_id(pool: &PgPool) -> EtlResult<TableId> {
    let table_id: SqlxTableId = sqlx::query_scalar(
        "SELECT c.oid
         FROM pg_class c
         JOIN pg_namespace n ON c.relnamespace = n.oid
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(SCHEMA_NAME)
    .bind(EVENTS_TABLE)
    .fetch_one(pool)
    .await?;

    Ok(TableId::new(table_id.0))
}

/// Reads the events table's schema from the catalog, as etl's table sync would store it.
///
/// The stream decodes every event with this schema, so it must not depend on etl's table sync
/// having stored it: on a first start the apply worker can deliver events before the table sync
/// did, and a table sync that failed leaves no schema at all.
pub async fn fetch_events_table_schema(pool: &PgPool, table_id: TableId) -> EtlResult<TableSchema> {
    let rows: Vec<(String, SqlxTableId, i32, bool, bool)> = sqlx::query_as(
        "SELECT a.attname::text,
                a.atttypid,
                a.atttypmod,
                a.attnotnull,
                EXISTS (
                  SELECT 1
                  FROM pg_index i
                  WHERE i.indrelid = a.attrelid
                    AND i.indisprimary
                    AND a.attnum = ANY(i.indkey)
                )
         FROM pg_attribute a
         WHERE a.attrelid = $1
           AND a.attnum > 0
           AND NOT a.attisdropped
         ORDER BY a.attnum",
    )
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_all(pool)
    .await?;

    let column_schemas = rows
        .into_iter()
        .map(|(name, type_oid, modifier, not_null, primary)| {
            ColumnSchema::new(
                name,
                convert_type_oid_to_type(type_oid.0),
                modifier,
                !not_null,
                primary,
            )
        })
        .collect();

    Ok(TableSchema::new(
        table_id,
        TableName::new(SCHEMA_NAME.to_string(), EVENTS_TABLE.to_string()),
        column_schemas,
    ))
}
