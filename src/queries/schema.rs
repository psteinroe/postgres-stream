use etl::{error::EtlResult, types::TableId};
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
