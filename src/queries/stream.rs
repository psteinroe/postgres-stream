use chrono::{DateTime, Utc};
use sqlx::{PgPool, prelude::FromRow};

#[derive(FromRow)]
pub struct StreamStateRow {
    pub id: i64,
    pub next_maintenance_at: DateTime<Utc>,
    pub failover_checkpoint_id: Option<String>,
    pub failover_checkpoint_ts: Option<DateTime<Utc>>,
    // Event data from left join
    pub event_id: Option<String>,
    pub event_created_at: Option<DateTime<Utc>>,
    pub event_payload: Option<serde_json::Value>,
    pub event_metadata: Option<serde_json::Value>,
    pub event_stream_id: Option<i64>,
    pub event_lsn: Option<String>,
}

/// Fetches current state row for a stream with optional event data.
pub async fn fetch_stream_state(
    pool: &PgPool,
    stream_id: i64,
) -> sqlx::Result<Option<StreamStateRow>> {
    sqlx::query_as(
        r#"
          select
            s.id,
            s.failover_checkpoint_id,
            s.failover_checkpoint_ts,
            s.next_maintenance_at,
            e.id::text as event_id,
            e.created_at as event_created_at,
            e.payload as event_payload,
            e.metadata as event_metadata,
            e.stream_id as event_stream_id,
            e.lsn::text as event_lsn
          from pgstream.streams s
          left join pgstream.events e
            on e.id::text = s.failover_checkpoint_id
            and e.created_at = s.failover_checkpoint_ts
          where s.id = $1
          "#,
    )
    .bind(stream_id)
    .fetch_optional(pool)
    .await
}

/// Upserts stream status (failover checkpoint).
pub async fn upsert_stream_status(
    pool: &PgPool,
    stream_id: i64,
    failover_checkpoint_id: Option<String>,
    failover_checkpoint_ts: Option<DateTime<Utc>>,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
          insert into pgstream.streams (id, failover_checkpoint_id, failover_checkpoint_ts)
          values ($1, $2, $3)
          on conflict (id) do update
            set failover_checkpoint_id = $2,
                failover_checkpoint_ts = $3
          "#,
    )
    .bind(stream_id)
    .bind(failover_checkpoint_id)
    .bind(failover_checkpoint_ts)
    .execute(pool)
    .await?;
    Ok(())
}

/// Upserts stream maintenance timestamp.
pub async fn upsert_stream_maintenance(
    pool: &PgPool,
    stream_id: i64,
    next_maintenance_at: DateTime<Utc>,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
          insert into pgstream.streams (id, next_maintenance_at)
          values ($1, $2)
          on conflict (id) do update
            set next_maintenance_at = $2
          "#,
    )
    .bind(stream_id)
    .bind(next_maintenance_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_stream_state(
    pool: &PgPool,
    stream_id: i64,
    next_maintenance_at: DateTime<Utc>,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        insert into pgstream.streams (id, next_maintenance_at, failover_checkpoint_id, failover_checkpoint_ts)
        values ($1, $2, null, null)
        "#,
    )
    .bind(stream_id)
    .bind(next_maintenance_at)
    .execute(pool)
    .await?;
    Ok(())
}
