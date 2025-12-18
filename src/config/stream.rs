use etl::config::{
    BatchConfig, PgConnectionConfig, PgConnectionConfigWithoutSecrets, PipelineConfig,
};
use serde::{Deserialize, Serialize};

use crate::types::{PublicationName, StreamId};

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
}

impl From<StreamConfig> for StreamConfigWithoutSecrets {
    fn from(value: StreamConfig) -> Self {
        StreamConfigWithoutSecrets {
            id: value.id,
            pg_connection: value.pg_connection.into(),
            batch: value.batch,
        }
    }
}

impl From<StreamConfig> for PipelineConfig {
    fn from(value: StreamConfig) -> Self {
        PipelineConfig {
            id: value.id,
            publication_name: value.id.publication_name(),
            pg_connection: value.pg_connection,
            batch: value.batch,
            // we only care about the events table, so 1 worker is sufficient
            max_table_sync_workers: 1,
            // todo
            table_error_retry_delay_ms: 1000,
            table_error_retry_max_attempts: 5,
        }
    }
}
