//! Redis Streams sink for publishing events to a Redis stream.
//!
//! Appends each event's payload to a Redis stream determined by:
//! 1. `stream` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `stream_name` in sink config
//!
//! # Message Format
//!
//! Each event is added to the stream using `XADD` with:
//! - Auto-generated stream entry ID (`*`)
//! - Single field: `payload` containing the JSON-serialized event payload
//!
//! Example entry when read with `XREAD`:
//! ```text
//! 1704067200000-0 payload {"id": 1, "name": "test"}
//! ```
//!
//! # Dynamic Routing
//!
//! The target stream can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "stream", "expression": "''events:'' || table_name"}
//! ]'
//! ```

use etl::error::EtlResult;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the Redis Streams sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (URL credentials) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct RedisStreamsSinkConfig {
    /// Redis connection URL (e.g., "redis://localhost:6379").
    /// Contains credentials and should be treated as sensitive.
    pub url: String,

    /// Name of the Redis stream to append events to. Optional if provided via event metadata.
    #[serde(default)]
    pub stream_name: Option<String>,

    /// Maximum stream length (optional). Uses MAXLEN ~ for approximate trimming.
    #[serde(default)]
    pub max_len: Option<usize>,
}

/// Configuration for the Redis Streams sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisStreamsSinkConfigWithoutSecrets {
    /// Name of the Redis stream to append events to (if configured).
    pub stream_name: Option<String>,

    /// Maximum stream length (optional).
    pub max_len: Option<usize>,
}

impl From<RedisStreamsSinkConfig> for RedisStreamsSinkConfigWithoutSecrets {
    fn from(config: RedisStreamsSinkConfig) -> Self {
        Self {
            stream_name: config.stream_name,
            max_len: config.max_len,
        }
    }
}

impl From<&RedisStreamsSinkConfig> for RedisStreamsSinkConfigWithoutSecrets {
    fn from(config: &RedisStreamsSinkConfig) -> Self {
        Self {
            stream_name: config.stream_name.clone(),
            max_len: config.max_len,
        }
    }
}

/// Sink that appends events to a Redis stream.
///
/// Each event's payload is added using XADD with a single "payload" field.
/// The sink uses a connection manager for automatic reconnection handling.
#[derive(Clone)]
pub struct RedisStreamsSink {
    /// Shared Redis connection manager.
    connection: Arc<Mutex<ConnectionManager>>,

    /// Default stream name. Can be overridden per-event via metadata.
    stream_name: Option<String>,

    /// Maximum stream length for approximate trimming.
    max_len: Option<usize>,
}

impl RedisStreamsSink {
    /// Creates a new Redis Streams sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis connection cannot be established.
    pub async fn new(
        config: RedisStreamsSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = redis::Client::open(config.url)?;
        let connection = ConnectionManager::new(client).await?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            stream_name: config.stream_name,
            max_len: config.max_len,
        })
    }

    /// Resolves the stream name for an event from metadata or config.
    fn resolve_stream_name<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic stream.
        if let Some(ref metadata) = event.metadata
            && let Some(stream) = metadata.get("stream").and_then(|v| v.as_str())
        {
            return Some(stream);
        }
        // Fall back to config stream name.
        self.stream_name.as_deref()
    }
}

impl Sink for RedisStreamsSink {
    fn name() -> &'static str {
        "redis-streams"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Build pipeline in a single pass - no pre-collection needed.
        let mut conn = self.connection.lock().await;
        let mut pipe = redis::pipe();

        for event in events {
            let stream_name = self.resolve_stream_name(&event).ok_or_else(|| {
                etl::etl_error!(
                    etl::error::ErrorKind::ConfigError,
                    "No stream_name configured",
                    "Stream name must be provided in sink config or event metadata (stream key)"
                )
            })?;

            let mut cmd = redis::cmd("XADD");
            cmd.arg(stream_name); // Copies internally.

            if let Some(max_len) = self.max_len {
                cmd.arg("MAXLEN").arg("~").arg(max_len);
            }

            cmd.arg("*").arg("payload").arg(event.payload.to_string());
            pipe.add_command(cmd);
        }

        pipe.query_async::<()>(&mut *conn).await.map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::DestinationError,
                "Failed to XADD events to Redis stream",
                e.to_string()
            )
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_name() {
        assert_eq!(RedisStreamsSink::name(), "redis-streams");
    }
}
