use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};

#[derive(FromRow)]
pub struct EventRow {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub payload: serde_json::Value,
    pub metadata: Option<serde_json::Value>,
    pub stream_id: i64,
}

/// Fetches a single event by its ID and creation timestamp.
pub async fn fetch_event(
    pool: &PgPool,
    event_id: &str,
    event_created_at: &DateTime<Utc>,
) -> sqlx::Result<EventRow> {
    sqlx::query_as(
        r#"
        select id::text, created_at, payload, metadata, stream_id
        from pgstream.events
        where id = $1::uuid and created_at = $2
        "#,
    )
    .bind(event_id)
    .bind(event_created_at)
    .fetch_one(pool)
    .await
}
