use chrono::Utc;
use etl::types::{Cell, Event, InsertEvent, PgLsn, TableId, TableRow};
use uuid::Uuid;

use crate::config::StreamConfig;
use crate::test_utils::TestDatabase;
use crate::types::EventIdentifier;

/// Helper to create a test stream configuration
#[must_use]
pub fn test_stream_config(db: &TestDatabase) -> StreamConfig {
    StreamConfig {
        id: 1,
        pg_connection: db.config.clone(),
        batch: etl::config::BatchConfig {
            max_size: 100,
            max_fill_ms: 1000,
        },
    }
}

/// Helper to create a test event with the pgstream.events table structure
/// Column order: id, payload, metadata, stream_id, created_at
#[must_use]
pub fn make_test_event(table_id: TableId, payload: serde_json::Value) -> Event {
    // Use today's date with a fixed time for deterministic tests
    let today = Utc::now().date_naive();
    let timestamp = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();

    Event::Insert(InsertEvent {
        start_lsn: PgLsn::from(0),
        commit_lsn: PgLsn::from(0),
        table_id,
        table_row: TableRow {
            values: vec![
                Cell::Uuid(Uuid::new_v4()),   // id
                Cell::Json(payload),          // payload
                Cell::Null,                   // metadata
                Cell::I64(1),                 // stream_id
                Cell::TimestampTz(timestamp), // created_at
            ],
        },
    })
}

/// Helper to create event with specific ID and timestamp (matching database record)
/// Column order: id, payload, metadata, stream_id, created_at
#[must_use]
pub fn make_event_with_id(
    table_id: TableId,
    id: &EventIdentifier,
    payload: serde_json::Value,
) -> Event {
    Event::Insert(InsertEvent {
        start_lsn: PgLsn::from(0),
        commit_lsn: PgLsn::from(0),
        table_id,
        table_row: TableRow {
            values: vec![
                Cell::Uuid(Uuid::parse_str(&id.id).unwrap()), // id
                Cell::Json(payload),                          // payload
                Cell::Null,                                   // metadata
                Cell::I64(1),                                 // stream_id
                Cell::TimestampTz(id.created_at),             // created_at
            ],
        },
    })
}

/// Helper to insert events directly into pgstream.events table
pub async fn insert_events_to_db(db: &TestDatabase, count: usize) -> Vec<EventIdentifier> {
    db.ensure_today_partition().await;

    let mut event_ids = Vec::new();
    // Use today's date with a fixed time to ensure deterministic ordering in tests
    let today = Utc::now().date_naive();
    let base_time = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_local_timezone(Utc)
        .unwrap();

    for i in 0..count {
        let id = Uuid::new_v4();
        // Use seconds for spacing to ensure distinct timestamps
        let created_at = base_time + chrono::Duration::seconds(i as i64);
        let payload = serde_json::json!({"seq": i});

        sqlx::query(
            "insert into pgstream.events (id, created_at, payload, stream_id) values ($1::uuid, $2, $3, $4)",
        )
        .bind(id.to_string())
        .bind(created_at)
        .bind(&payload)
        .bind(1i64)
        .execute(&db.pool)
        .await
        .unwrap();

        event_ids.push(EventIdentifier::new(id.to_string(), created_at));
    }

    event_ids
}
