use etl::config::{
    BatchConfig as EtlBatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig,
    PgConnectionConfig, PgConnectionConfigWithoutSecrets, PipelineConfig, ReplicationSlotConfig,
    TableSyncCopyConfig,
};
use serde::{Deserialize, Serialize};

use crate::types::{PublicationName, StreamId};

/// Batch processing configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum number of events published to the sink in a single batch.
    #[serde(default = "default_batch_max_size")]
    pub max_size: usize,
    /// Maximum time, in milliseconds, to wait before flushing a partially filled batch.
    #[serde(default = "default_batch_max_fill_ms")]
    pub max_fill_ms: u64,
}

impl BatchConfig {
    /// Default maximum number of events per batch.
    pub const DEFAULT_MAX_SIZE: usize = 100_000;

    /// Default maximum fill time in milliseconds.
    pub const DEFAULT_MAX_FILL_MS: u64 = 10_000;
}

const fn default_batch_max_size() -> usize {
    BatchConfig::DEFAULT_MAX_SIZE
}

const fn default_batch_max_fill_ms() -> u64 {
    BatchConfig::DEFAULT_MAX_FILL_MS
}

impl From<BatchConfig> for EtlBatchConfig {
    fn from(value: BatchConfig) -> Self {
        EtlBatchConfig {
            max_fill_ms: value.max_fill_ms,
            ..EtlBatchConfig::default()
        }
    }
}

/// Configuration for a Postgres stream.
///
/// Contains all settings required to run a Postgres replication stream including
/// source database connection and batching parameters.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct StreamConfig {
    /// The unique identifier for this stream/pipeline.
    ///
    /// A stream id determines isolation between streams, in terms of replication slots and state
    /// store.
    pub id: StreamId,
    /// The connection configuration for the Postgres instance to which the stream connects for
    /// replication.
    pub pg_connection: PgConnectionConfig,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Whether the replication slot is a failover slot.
    ///
    /// Requires Postgres 17 or newer. New slots are created with `FAILOVER`, and an existing
    /// slot is altered to be a failover slot on startup.
    #[serde(default)]
    pub failover: bool,
}

/// Same as [`PostgresStreamConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamConfigWithoutSecrets {
    /// The unique identifier for this stream/pipeline.
    ///
    /// A stream id determines isolation between streams, in terms of replication slots and state
    /// store.
    pub id: StreamId,
    /// The connection configuration for the Postgres instance to which the stream connects for
    /// replication.
    pub pg_connection: PgConnectionConfigWithoutSecrets,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Whether the replication slot is a failover slot.
    #[serde(default)]
    pub failover: bool,
}

impl From<StreamConfig> for StreamConfigWithoutSecrets {
    fn from(value: StreamConfig) -> Self {
        StreamConfigWithoutSecrets {
            id: value.id,
            pg_connection: value.pg_connection.into(),
            batch: value.batch,
            failover: value.failover,
        }
    }
}

impl From<StreamConfig> for PipelineConfig {
    fn from(value: StreamConfig) -> Self {
        PipelineConfig {
            id: value.id,
            publication_name: value.id.publication_name(),
            pg_connection: value.pg_connection,
            // the state store lives in the source database
            store_pg_connection: None,
            replication_slot: ReplicationSlotConfig {
                failover: value.failover,
                ..ReplicationSlotConfig::default()
            },
            batch: value.batch.into(),
            // we only care about the events table, so 1 worker is sufficient
            max_table_sync_workers: 1,
            // todo
            table_error_retry_delay_ms: 1000,
            table_error_retry_max_attempts: 5,
            // pgstream discards table copies (write_table_rows is a no-op), skip them entirely
            table_sync_copy: TableSyncCopyConfig::SkipAllTables,
            // single table, serial copy is fine
            max_copy_connections_per_table: 1,
            // we handle recovery ourselves with checkpoint preservation
            invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
            // the event trigger installed by etl's source migrations requires superuser.
            // pgstream installs the schema helper functions etl calls at runtime in its own
            // migrations instead, and announces schema changes of the events table with
            // `pgstream.emit_events_schema_change()`.
            run_source_migrations: false,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            replication_lag_refresh_interval_ms:
                PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS,
            settle_durable_interval_ms: None,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            drain_on_shutdown: PipelineConfig::DEFAULT_DRAIN_ON_SHUTDOWN,
            shutdown_drain_timeout_ms: PipelineConfig::DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
        }
    }
}
