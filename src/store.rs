use chrono::{DateTime, Utc};
use etl::{
    config::{IntoConnectOptions, PgConnectionConfig},
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::{SnapshotId, TableId, TableSchema},
    store::SchemaStore,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    config::{STATE_MANAGEMENT_OPTIONS, StreamConfig},
    maintenance::PartitionInfo,
    queries::{
        create_partition, drop_partition, fetch_event, fetch_events_table_id, fetch_stream_state,
        insert_stream_state, list_partitions, upsert_stream_maintenance, upsert_stream_status,
    },
    slot_recovery::resolve_slot_recovery,
    types::{EventIdentifier, StreamStatus, TriggeredEvent},
};

const NUM_POOL_CONNECTIONS: u32 = 1;

/// Core mutable state of the stream.
#[derive(Debug)]
struct StreamState {
    next_maintenance_at: DateTime<Utc>,
    status: StreamStatus,
    /// Inclusive end of the failover replay window, see [`StreamStore::extend_failover_window`].
    failover_window_end: Option<EventIdentifier>,
    /// Whether slot recovery left a replay lower bound to resolve on the next batch.
    slot_recovery_pending: bool,
}

/// Returns the earliest and the latest event of a batch by `(created_at, id)`.
#[must_use]
pub fn replay_bounds(events: &[TriggeredEvent]) -> Option<(EventIdentifier, EventIdentifier)> {
    let first = events
        .iter()
        .map(|e| &e.id)
        .min_by(|a, b| a.cmp_replay_order(b))?;
    let last = events
        .iter()
        .map(|e| &e.id)
        .max_by(|a, b| a.cmp_replay_order(b))?;
    Some((first.clone(), last.clone()))
}

#[derive(Debug)]
pub struct StreamStore<S> {
    stream_config: StreamConfig,
    schema_store: Arc<S>,
    state: Arc<RwLock<StreamState>>,
    checkpoint_cache: Arc<RwLock<Option<Arc<TriggeredEvent>>>>,
    events_table_id: Arc<TableId>,
}

impl<S> Clone for StreamStore<S> {
    fn clone(&self) -> Self {
        Self {
            stream_config: self.stream_config.clone(),
            schema_store: Arc::clone(&self.schema_store),
            state: Arc::clone(&self.state),
            checkpoint_cache: Arc::clone(&self.checkpoint_cache),
            events_table_id: Arc::clone(&self.events_table_id),
        }
    }
}

impl<S: SchemaStore> StreamStore<S> {
    pub async fn create(stream_config: StreamConfig, schema_store: S) -> EtlResult<Self> {
        let mut state = StreamState {
            next_maintenance_at: DateTime::UNIX_EPOCH,
            status: StreamStatus::Healthy,
            failover_window_end: None,
            slot_recovery_pending: false,
        };

        let mut checkpoint_cache = None;

        let pool = Self::connect_temporary(&stream_config.pg_connection).await?;
        let maybe_row = fetch_stream_state(&pool, stream_config.id as i64).await?;

        if let Some(row) = maybe_row {
            state.next_maintenance_at = row.next_maintenance_at;
            state.slot_recovery_pending = row.slot_recovery_lsn.is_some();

            if let (Some(event_id), Some(event_created_at)) = (row.event_id, row.event_created_at) {
                let checkpoint_event_id = EventIdentifier::new(event_id, event_created_at);

                // Pre-populate cache if payload and stream_id are available
                if let (Some(payload), Some(event_stream_id)) =
                    (row.event_payload, row.event_stream_id)
                {
                    checkpoint_cache = Some(Arc::new(TriggeredEvent {
                        id: checkpoint_event_id.clone(),
                        payload,
                        metadata: row.event_metadata,
                        stream_id: crate::types::StreamId::from(event_stream_id as u64),
                        commit_lsn: None,
                        lsn: row.event_lsn.and_then(|s| s.parse().ok()),
                    }));
                }

                state.status = StreamStatus::Failover {
                    checkpoint_event_id,
                };
                state.failover_window_end = row
                    .failover_window_end_id
                    .zip(row.failover_window_end_ts)
                    .map(|(id, created_at)| EventIdentifier::new(id, created_at));
            } else {
                state.status = StreamStatus::Healthy;
            }
        } else {
            let now = Utc::now();
            insert_stream_state(&pool, stream_config.id as i64, now).await?;

            state.status = StreamStatus::Healthy;
            state.next_maintenance_at = now;
        }

        let events_table_id = fetch_events_table_id(&pool).await?;

        Ok(Self {
            stream_config,
            schema_store: Arc::new(schema_store),
            state: Arc::new(RwLock::new(state)),
            checkpoint_cache: Arc::new(RwLock::new(checkpoint_cache)),
            events_table_id: Arc::new(events_table_id),
        })
    }

    /// Acquires a temporary connection that will be closed when dropped.
    async fn connect_temporary(source_config: &PgConnectionConfig) -> EtlResult<PgPool> {
        let options = source_config.with_db(Some(&STATE_MANAGEMENT_OPTIONS));

        let pool = PgPoolOptions::new()
            .min_connections(NUM_POOL_CONNECTIONS)
            .max_connections(NUM_POOL_CONNECTIONS)
            .connect_with(options)
            .await?;

        Ok(pool)
    }

    /// Returns the current stream state from memory.
    pub async fn get_stream_state(&self) -> EtlResult<(StreamStatus, DateTime<Utc>)> {
        let state = self.state.read().await;
        Ok((state.status.clone(), state.next_maintenance_at))
    }

    /// Returns the inclusive end of the failover replay window from memory.
    pub async fn get_failover_window_end(&self) -> Option<EventIdentifier> {
        self.state.read().await.failover_window_end.clone()
    }

    /// Write stream status into memory and to Postgres.
    ///
    /// Becoming healthy clears the failover replay window, setting a failover checkpoint keeps
    /// its end.
    pub async fn store_stream_status(&self, status: StreamStatus) -> EtlResult<()> {
        let window_end = match &status {
            StreamStatus::Failover { .. } => self.get_failover_window_end().await,
            StreamStatus::Healthy => None,
        };
        self.store_failover_window(status, window_end).await
    }

    /// Extends the failover replay window to cover a batch, entering failover if healthy.
    ///
    /// Call this for every batch that failed to publish or was dropped during failover, before
    /// reporting it as written, with the batch's [`replay_bounds`]. The window's start (the
    /// failover checkpoint) becomes the earliest and its end the latest of the window and the
    /// batch by `(created_at, id)`. Batches arrive in commit order, so their events can lie
    /// anywhere relative to the window. Returns whether the stream entered failover.
    pub async fn extend_failover_window(
        &self,
        (first, last): &(EventIdentifier, EventIdentifier),
    ) -> EtlResult<bool> {
        let (status, window_end) = {
            let state = self.state.read().await;
            (state.status.clone(), state.failover_window_end.clone())
        };

        let (entered_failover, start, end) = match status {
            StreamStatus::Healthy => (true, first.clone(), last.clone()),
            StreamStatus::Failover {
                checkpoint_event_id,
            } => {
                let start = std::cmp::min_by(checkpoint_event_id, first.clone(), |a, b| {
                    a.cmp_replay_order(b)
                });
                // A checkpoint without an end (written by an older version) replayed only up to
                // the batch that ends failover. The start bounds what it covered.
                let end = window_end.unwrap_or_else(|| start.clone());
                let end = std::cmp::max_by(end, last.clone(), |a, b| a.cmp_replay_order(b));
                (false, start, end)
            }
        };

        self.store_failover_window(
            StreamStatus::Failover {
                checkpoint_event_id: start,
            },
            Some(end),
        )
        .await?;

        Ok(entered_failover)
    }

    async fn store_failover_window(
        &self,
        status: StreamStatus,
        window_end: Option<EventIdentifier>,
    ) -> EtlResult<()> {
        let (checkpoint_id, checkpoint_ts) = match &status {
            StreamStatus::Failover {
                checkpoint_event_id,
            } => (
                Some(checkpoint_event_id.id.clone()),
                Some(checkpoint_event_id.created_at),
            ),
            StreamStatus::Healthy => (None, None),
        };
        let (window_end_id, window_end_ts) =
            window_end.clone().map(|e| (e.id, e.created_at)).unzip();

        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        upsert_stream_status(
            &pool,
            self.stream_config.id as i64,
            checkpoint_id,
            checkpoint_ts,
            window_end_id,
            window_end_ts,
        )
        .await?;

        let mut state = self.state.write().await;
        state.status = status;
        state.failover_window_end = window_end;
        Ok(())
    }

    /// Resolves the replay lower bound left by slot recovery into a failover checkpoint.
    ///
    /// Does nothing unless slot recovery ran before this store was created. Returns whether the
    /// stream entered failover. See [`resolve_slot_recovery`].
    pub async fn resolve_pending_slot_recovery(&self) -> EtlResult<bool> {
        if !self.state.read().await.slot_recovery_pending {
            return Ok(false);
        }

        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        let window = resolve_slot_recovery(&pool, self.stream_config.id).await?;

        let mut state = self.state.write().await;
        state.slot_recovery_pending = false;

        let Some(window) = window else {
            return Ok(false);
        };
        let entered_failover = matches!(state.status, StreamStatus::Healthy);
        state.status = StreamStatus::Failover {
            checkpoint_event_id: window.start,
        };
        state.failover_window_end = Some(window.end);

        Ok(entered_failover)
    }

    /// Get checkpoint event from DB (with caching)
    pub async fn get_checkpoint_event(
        &self,
        checkpoint_id: &EventIdentifier,
    ) -> EtlResult<Arc<TriggeredEvent>> {
        // Check cache
        {
            let cache = self.checkpoint_cache.read().await;
            if let Some(ref cached) = *cache
                && &cached.id == checkpoint_id
            {
                return Ok(Arc::clone(cached));
            }
        }

        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        let (event_id, event_created_at) = checkpoint_id.primary_keys();
        let row = fetch_event(&pool, &event_id, &event_created_at).await?;

        let event = Arc::new(TriggeredEvent {
            id: EventIdentifier::new(row.id, row.created_at),
            payload: row.payload,
            metadata: row.metadata,
            stream_id: crate::types::StreamId::from(row.stream_id as u64),
            commit_lsn: None,
            lsn: row.lsn.and_then(|s| s.parse().ok()),
        });

        let mut cache = self.checkpoint_cache.write().await;
        *cache = Some(Arc::clone(&event));

        Ok(event)
    }

    /// Returns the stream ID for this store.
    #[must_use]
    pub fn stream_id(&self) -> u64 {
        self.stream_config.id
    }

    pub async fn store_next_maintenance_at(&self, ts: DateTime<Utc>) -> EtlResult<()> {
        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        upsert_stream_maintenance(&pool, self.stream_config.id as i64, ts).await?;

        let mut state = self.state.write().await;
        state.next_maintenance_at = ts;
        Ok(())
    }

    pub async fn load_partitions(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> EtlResult<Vec<PartitionInfo>> {
        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        let records = list_partitions(&pool, schema_name, table_name).await?;

        records
            .into_iter()
            .map(|record| PartitionInfo::from_name(record.partition_name))
            .collect()
    }

    pub async fn create_partition(
        &self,
        schema_name: &str,
        table_name: &str,
        partition_info: &PartitionInfo,
    ) -> EtlResult<()> {
        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        let (start, end) = partition_info.range_bounds();

        create_partition(
            &pool,
            schema_name,
            table_name,
            &partition_info.name,
            &start,
            &end,
        )
        .await?;
        Ok(())
    }

    pub async fn delete_partition(&self, schema_name: &str, partition_name: &str) -> EtlResult<()> {
        let pool = Self::connect_temporary(&self.stream_config.pg_connection).await?;
        drop_partition(&pool, schema_name, partition_name).await?;
        Ok(())
    }

    pub async fn get_events_table_schema(&self) -> EtlResult<Arc<TableSchema>> {
        self.schema_store
            .get_table_schema(&self.events_table_id, SnapshotId::max())
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Events table schema not found",
                    format!("Schema for table_id={} not in cache", self.events_table_id)
                )
            })
    }
}
