use chrono::Utc;
use postgres_stream::config::StreamConfig;
use postgres_stream::store::StreamStore;
use postgres_stream::test_utils::{TestDatabase, create_postgres_store, test_stream_config};
use postgres_stream::types::{EventIdentifier, StreamStatus};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn test_store_initializes_healthy_status_for_new_stream() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let store_backend = create_postgres_store(config.id, &db.config, &db.pool).await;

    let store = StreamStore::create(config, store_backend)
        .await
        .expect("Failed to create store");

    let (status, _) = store.get_stream_state().await.unwrap();

    assert!(
        matches!(status, StreamStatus::Healthy),
        "New stream should start in Healthy status"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_persists_failover_status() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let store_backend = create_postgres_store(config.id, &db.config, &db.pool).await;

    let store = StreamStore::create(config.clone(), store_backend.clone())
        .await
        .expect("Failed to create store");

    // Ensure partition exists for today
    db.ensure_today_partition().await;

    // Insert checkpoint event into database
    let event_id = Uuid::new_v4();
    let created_at = Utc::now();
    let payload = serde_json::json!({"checkpoint": "data"});

    sqlx::query(
        "insert into pgstream.events (id, created_at, payload, stream_id) values ($1::uuid, $2, $3, $4)",
    )
    .bind(event_id.to_string())
    .bind(created_at)
    .bind(&payload)
    .bind(1i64)
    .execute(&db.pool)
    .await
    .unwrap();

    // Create checkpoint event ID
    let checkpoint_id = EventIdentifier::new(event_id.to_string(), created_at);

    // Store failover status
    store
        .store_stream_status(StreamStatus::Failover {
            checkpoint_event_id: checkpoint_id.clone(),
        })
        .await
        .unwrap();

    // Create new store instance to verify persistence
    let store2 = StreamStore::create(config, store_backend).await.unwrap();

    let (status, _) = store2.get_stream_state().await.unwrap();

    match status {
        StreamStatus::Failover {
            checkpoint_event_id,
        } => {
            assert_eq!(checkpoint_event_id.id, checkpoint_id.id);
            // Compare timestamps at microsecond precision (Postgres precision)
            assert_eq!(
                checkpoint_event_id.created_at.timestamp_micros(),
                checkpoint_id.created_at.timestamp_micros()
            );
        }
        StreamStatus::Healthy => panic!("Expected Failover status to be persisted"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_transitions_failover_to_healthy() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let store_backend = create_postgres_store(config.id, &db.config, &db.pool).await;

    let store = StreamStore::create(config, store_backend)
        .await
        .expect("Failed to create store");

    // Start in failover
    let checkpoint_id = EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now());
    store
        .store_stream_status(StreamStatus::Failover {
            checkpoint_event_id: checkpoint_id,
        })
        .await
        .unwrap();

    // Transition to healthy
    store
        .store_stream_status(StreamStatus::Healthy)
        .await
        .unwrap();

    let (status, _) = store.get_stream_state().await.unwrap();

    assert!(
        matches!(status, StreamStatus::Healthy),
        "Should transition from Failover to Healthy"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_persists_next_maintenance_timestamp() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let store_backend = create_postgres_store(config.id, &db.config, &db.pool).await;

    let store = StreamStore::create(config.clone(), store_backend.clone())
        .await
        .expect("Failed to create store");

    // Store future maintenance time
    let future_time = Utc::now() + chrono::Duration::days(1);
    store.store_next_maintenance_at(future_time).await.unwrap();

    // Create new store instance to verify persistence
    let store2 = StreamStore::create(config, store_backend).await.unwrap();

    let (_, next_maintenance) = store2.get_stream_state().await.unwrap();

    assert_eq!(
        next_maintenance.timestamp(),
        future_time.timestamp(),
        "Next maintenance time should be persisted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_caches_checkpoint_event() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let store_backend = create_postgres_store(config.id, &db.config, &db.pool).await;

    let store = StreamStore::create(config, store_backend)
        .await
        .expect("Failed to create store");

    // Ensure partition exists for today
    db.ensure_today_partition().await;

    // Insert event into database and fetch back the timestamp (Postgres microsecond precision)
    let event_id = Uuid::new_v4();
    let payload = serde_json::json!({"test": "data"});

    let created_at: chrono::DateTime<Utc> = sqlx::query_scalar(
        "insert into pgstream.events (id, created_at, payload, stream_id) values ($1::uuid, now(), $2, $3) returning created_at",
    )
    .bind(event_id.to_string())
    .bind(&payload)
    .bind(1i64)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    // Set failover status with this event
    let checkpoint_id = EventIdentifier::new(event_id.to_string(), created_at);
    store
        .store_stream_status(StreamStatus::Failover {
            checkpoint_event_id: checkpoint_id.clone(),
        })
        .await
        .unwrap();

    // Fetch checkpoint event twice
    let event1 = store.get_checkpoint_event(&checkpoint_id).await.unwrap();
    let event2 = store.get_checkpoint_event(&checkpoint_id).await.unwrap();

    // Should be the same Arc (cached)
    assert!(
        std::sync::Arc::ptr_eq(&event1, &event2),
        "Checkpoint event should be cached"
    );
    assert_eq!(event1.payload, payload);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_events_table_schema_available() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let store_backend = create_postgres_store(config.id, &db.config, &db.pool).await;

    let store = StreamStore::create(config, store_backend)
        .await
        .expect("Failed to create store");

    // Get events table schema
    let schema = store.get_events_table_schema().await.unwrap();

    // Verify it has the expected columns (id, created_at, payload, metadata, stream_id, lsn)
    assert_eq!(schema.column_schemas.len(), 6);

    let column_names: Vec<&str> = schema
        .column_schemas
        .iter()
        .map(|c| c.name.as_str())
        .collect();

    assert!(column_names.contains(&"id"));
    assert!(column_names.contains(&"created_at"));
    assert!(column_names.contains(&"payload"));
    assert!(column_names.contains(&"metadata"));
    assert!(column_names.contains(&"stream_id"));
    assert!(column_names.contains(&"lsn"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_multiple_streams_isolated() {
    let db = TestDatabase::spawn().await;

    // Create two different stream stores
    let config1 = StreamConfig {
        id: 1,
        pg_connection: db.config.clone(),
        batch: etl::config::BatchConfig {
            max_size: 100,
            max_fill_ms: 1000,
        },
    };

    let config2 = StreamConfig {
        id: 2,
        pg_connection: db.config.clone(),
        batch: etl::config::BatchConfig {
            max_size: 100,
            max_fill_ms: 1000,
        },
    };

    let store1 = StreamStore::create(
        config1.clone(),
        create_postgres_store(config1.id, &db.config, &db.pool).await,
    )
    .await
    .unwrap();

    let store2 = StreamStore::create(
        config2.clone(),
        create_postgres_store(config2.id, &db.config, &db.pool).await,
    )
    .await
    .unwrap();

    // Set different states for each
    let checkpoint_id = EventIdentifier::new(Uuid::new_v4().to_string(), Utc::now());
    store1
        .store_stream_status(StreamStatus::Failover {
            checkpoint_event_id: checkpoint_id,
        })
        .await
        .unwrap();

    store2
        .store_stream_status(StreamStatus::Healthy)
        .await
        .unwrap();

    // Verify they maintain separate states
    let (status1, _) = store1.get_stream_state().await.unwrap();
    let (status2, _) = store2.get_stream_state().await.unwrap();

    assert!(matches!(status1, StreamStatus::Failover { .. }));
    assert!(matches!(status2, StreamStatus::Healthy));
}
