use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, ColumnSchema, TableRow},
};

use etl::types::Event;

use crate::types::StreamId;

/// Unique identifier for an event.
///
/// Primary keys of the pgstream.events table.
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct EventIdentifier {
    pub id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl EventIdentifier {
    #[must_use]
    pub fn new(id: String, created_at: chrono::DateTime<chrono::Utc>) -> Self {
        Self { id, created_at }
    }

    #[must_use]
    pub fn primary_keys(&self) -> (String, chrono::DateTime<chrono::Utc>) {
        (self.id.clone(), self.created_at)
    }
}

impl PartialOrd for EventIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EventIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.created_at.cmp(&other.created_at))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggeredEvent {
    pub id: EventIdentifier,
    pub payload: serde_json::Value,
    pub metadata: Option<serde_json::Value>,
    pub stream_id: StreamId,
}

impl TriggeredEvent {
    #[must_use]
    pub fn primary_keys(&self) -> (EventIdentifier, chrono::DateTime<chrono::Utc>) {
        (self.id.clone(), self.id.created_at)
    }
}

macro_rules! missing {
    ($name:literal) => {
        etl_error!(
            ErrorKind::InvalidData,
            concat!("Missing ", $name),
            concat!(
                "The '",
                $name,
                "' column is required but was not found or is null"
            )
        )
    };
}

pub fn convert_event_from_table(
    table_row: &mut TableRow,
    column_schemas: &[ColumnSchema],
) -> EtlResult<TriggeredEvent> {
    let mut id = None;
    let mut created_at = None;
    let mut payload = None;
    let mut metadata = None;
    let mut stream_id = None;

    for (idx, col) in column_schemas.iter().enumerate() {
        let Some(cell) = table_row.values.get_mut(idx) else {
            continue;
        };

        match (col.name.as_str(), std::mem::replace(cell, Cell::Null)) {
            ("id", Cell::Uuid(v)) => id = Some(v.to_string()),
            ("created_at", Cell::TimestampTz(ts)) => created_at = Some(ts),
            ("payload", Cell::Json(v)) => payload = Some(v),
            ("metadata", Cell::Json(v)) => metadata = Some(v),
            ("stream_id", Cell::I64(v)) => stream_id = Some(StreamId::from(v as u64)),
            _ => {}
        }
    }

    Ok(TriggeredEvent {
        id: EventIdentifier::new(
            id.ok_or_else(|| missing!("id"))?,
            created_at.ok_or_else(|| missing!("created_at"))?,
        ),
        payload: payload.ok_or_else(|| missing!("payload"))?,
        stream_id: stream_id.ok_or_else(|| missing!("stream_id"))?,
        metadata,
    })
}

pub fn convert_events_from_table_rows(
    table_rows: Vec<TableRow>,
    column_schemas: &[ColumnSchema],
) -> EtlResult<Vec<TriggeredEvent>> {
    table_rows
        .into_iter()
        .map(|mut table_row| convert_event_from_table(&mut table_row, column_schemas))
        .collect()
}

pub fn convert_stream_events_from_events(
    events: Vec<Event>,
    column_schemas: &[ColumnSchema],
) -> EtlResult<Vec<TriggeredEvent>> {
    events
        .into_iter()
        .filter_map(|event| match event {
            Event::Insert(mut insert_event) => Some(convert_event_from_table(
                &mut insert_event.table_row,
                column_schemas,
            )),
            Event::Begin(_)
            | Event::Commit(_)
            | Event::Update(_)
            | Event::Delete(_)
            | Event::Relation(_)
            | Event::Truncate(_)
            | Event::Unsupported => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use etl::types::{InsertEvent, PgLsn, TableId};
    use tokio_postgres::types::Type;
    use uuid::Uuid;

    fn make_column_schemas() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("id".to_string(), Type::UUID, -1, false, true),
            ColumnSchema::new("created_at".to_string(), Type::TIMESTAMPTZ, -1, false, true),
            ColumnSchema::new("payload".to_string(), Type::JSONB, -1, true, false),
            ColumnSchema::new("metadata".to_string(), Type::JSONB, -1, true, false),
            ColumnSchema::new("stream_id".to_string(), Type::INT8, -1, false, false),
        ]
    }

    fn make_table_row(
        id: Uuid,
        created_at: chrono::DateTime<Utc>,
        payload: serde_json::Value,
    ) -> TableRow {
        TableRow {
            values: vec![
                Cell::Uuid(id),
                Cell::TimestampTz(created_at),
                Cell::Json(payload),
                Cell::Null,   // metadata
                Cell::I64(1), // stream_id
            ],
        }
    }

    #[test]
    fn test_convert_event_from_table_valid() {
        let column_schemas = make_column_schemas();
        let id = Uuid::new_v4();
        let created_at = Utc::now();
        let payload = serde_json::json!({"test": "data"});

        let mut table_row = make_table_row(id, created_at, payload.clone());

        let result = convert_event_from_table(&mut table_row, &column_schemas).unwrap();

        assert_eq!(result.id.id, id.to_string());
        assert_eq!(result.id.created_at, created_at);
        assert_eq!(result.payload, payload);
    }

    #[test]
    fn test_convert_event_from_table_missing_id() {
        let column_schemas = make_column_schemas();
        let mut table_row = TableRow {
            values: vec![
                Cell::Null, // Missing id
                Cell::TimestampTz(Utc::now()),
                Cell::Json(serde_json::json!({"test": "data"})),
                Cell::Null,
                Cell::I64(1),
            ],
        };

        let result = convert_event_from_table(&mut table_row, &column_schemas);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing id"));
    }

    #[test]
    fn test_convert_event_from_table_missing_created_at() {
        let column_schemas = make_column_schemas();
        let mut table_row = TableRow {
            values: vec![
                Cell::Uuid(Uuid::new_v4()),
                Cell::Null, // Missing created_at
                Cell::Json(serde_json::json!({"test": "data"})),
                Cell::Null,
                Cell::I64(1),
            ],
        };

        let result = convert_event_from_table(&mut table_row, &column_schemas);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing created_at")
        );
    }

    #[test]
    fn test_convert_event_from_table_missing_payload() {
        let column_schemas = make_column_schemas();
        let mut table_row = TableRow {
            values: vec![
                Cell::Uuid(Uuid::new_v4()),
                Cell::TimestampTz(Utc::now()),
                Cell::Null, // Missing payload
                Cell::Null,
                Cell::I64(1),
            ],
        };

        let result = convert_event_from_table(&mut table_row, &column_schemas);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing payload"));
    }

    #[test]
    fn test_convert_events_from_table_rows_multiple() {
        let column_schemas = make_column_schemas();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let ts1 = Utc::now();
        let ts2 = Utc::now();

        let table_rows = vec![
            make_table_row(id1, ts1, serde_json::json!({"num": 1})),
            make_table_row(id2, ts2, serde_json::json!({"num": 2})),
        ];

        let result = convert_events_from_table_rows(table_rows, &column_schemas).unwrap();

        assert_eq!(result.len(), 2);
        let first = result.first().expect("first element exists");
        assert_eq!(first.id.id, id1.to_string());
        assert_eq!(first.payload.get("num").and_then(|v| v.as_i64()), Some(1));
        let second = result.get(1).expect("second element exists");
        assert_eq!(second.id.id, id2.to_string());
        assert_eq!(second.payload.get("num").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn test_convert_events_from_table_rows_empty() {
        let column_schemas = make_column_schemas();
        let result = convert_events_from_table_rows(vec![], &column_schemas).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_convert_stream_events_from_events_filters_inserts_only() {
        let column_schemas = make_column_schemas();
        let id = Uuid::new_v4();
        let ts = Utc::now();

        let events = vec![
            Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(0),
                commit_lsn: PgLsn::from(0),
                table_id: TableId::new(1),
                table_row: make_table_row(id, ts, serde_json::json!({"test": 1})),
            }),
            Event::Unsupported,
            Event::Unsupported,
        ];

        let result = convert_stream_events_from_events(events, &column_schemas).unwrap();

        // Only the Insert event should be converted
        assert_eq!(result.len(), 1);
        let first = result.first().expect("first element exists");
        assert_eq!(first.id.id, id.to_string());
        assert_eq!(first.payload.get("test").and_then(|v| v.as_i64()), Some(1));
    }

    #[test]
    fn test_convert_stream_events_from_events_empty() {
        let column_schemas = make_column_schemas();
        let events = vec![Event::Unsupported, Event::Unsupported];

        let result = convert_stream_events_from_events(events, &column_schemas).unwrap();

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_convert_stream_events_from_events_error_propagates() {
        let column_schemas = make_column_schemas();

        // Insert event with missing id
        let events = vec![Event::Insert(InsertEvent {
            start_lsn: PgLsn::from(0),
            commit_lsn: PgLsn::from(0),
            table_id: TableId::new(1),
            table_row: TableRow {
                values: vec![
                    Cell::Null, // Missing id
                    Cell::TimestampTz(Utc::now()),
                    Cell::Json(serde_json::json!({"test": 1})),
                    Cell::Null,
                    Cell::I64(1),
                ],
            },
        })];

        let result = convert_stream_events_from_events(events, &column_schemas);

        assert!(result.is_err());
    }

    #[test]
    fn test_event_identifier_equality() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let id1 = EventIdentifier::new("event1".to_string(), ts);
        let id2 = EventIdentifier::new("event1".to_string(), ts);
        let id3 = EventIdentifier::new("event2".to_string(), ts);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_event_identifier_ordering_by_id() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let id1 = EventIdentifier::new("a".to_string(), ts);
        let id2 = EventIdentifier::new("b".to_string(), ts);

        assert!(id1 < id2);
        assert!(id2 > id1);
    }

    #[test]
    fn test_event_identifier_ordering_by_timestamp() {
        let ts1 = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let ts2 = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 13, 0, 0).unwrap();
        let id1 = EventIdentifier::new("event1".to_string(), ts1);
        let id2 = EventIdentifier::new("event1".to_string(), ts2);

        assert!(id1 < id2);
    }

    #[test]
    fn test_event_identifier_ordering_id_takes_precedence() {
        let ts1 = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let ts2 = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 13, 0, 0).unwrap();

        // Even though ts1 is earlier, "b" > "a"
        let id1 = EventIdentifier::new("b".to_string(), ts1);
        let id2 = EventIdentifier::new("a".to_string(), ts2);

        assert!(id1 > id2);
    }

    #[test]
    fn test_event_identifier_primary_keys() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let id = EventIdentifier::new("event1".to_string(), ts);

        let (id_str, created_at) = id.primary_keys();
        assert_eq!(id_str, "event1");
        assert_eq!(created_at, ts);
    }

    #[test]
    fn test_stream_event_equality() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let id = EventIdentifier::new("event1".to_string(), ts);
        let payload = serde_json::json!({"test": "value"});

        let event1 = TriggeredEvent {
            id: id.clone(),
            payload: payload.clone(),
            metadata: None,
            stream_id: StreamId::from(1u64),
        };
        let event2 = TriggeredEvent {
            id,
            payload,
            metadata: None,
            stream_id: StreamId::from(1u64),
        };

        assert_eq!(event1, event2);
    }

    #[test]
    fn test_stream_event_primary_keys() {
        let ts = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let id = EventIdentifier::new("event1".to_string(), ts);
        let payload = serde_json::json!({"test": "value"});

        let event = TriggeredEvent {
            id,
            payload,
            metadata: None,
            stream_id: StreamId::from(1u64),
        };
        let (id_returned, created_at) = event.primary_keys();

        assert_eq!(id_returned.id, "event1");
        assert_eq!(created_at, ts);
    }
}
