//! Redis Strings sink for publishing events as key-value pairs.
//!
//! Stores each event's payload as a Redis string. The key is determined by:
//! 1. `key` in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to event ID (optionally with key_prefix from config)
//!
//! # Dynamic Routing
//!
//! The Redis key can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "key", "expression": "''user:'' || (payload->''user_id'')::text"}
//! ]'
//! ```

use etl::error::EtlResult;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the Redis Strings sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (URL credentials) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct RedisStringsSinkConfig {
    /// Redis connection URL (e.g., "redis://localhost:6379").
    /// Contains credentials and should be treated as sensitive.
    pub url: String,

    /// Optional prefix for all keys.
    #[serde(default)]
    pub key_prefix: Option<String>,
}

/// Configuration for the Redis Strings sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisStringsSinkConfigWithoutSecrets {
    /// Optional prefix for all keys.
    pub key_prefix: Option<String>,
}

impl From<RedisStringsSinkConfig> for RedisStringsSinkConfigWithoutSecrets {
    fn from(config: RedisStringsSinkConfig) -> Self {
        Self {
            key_prefix: config.key_prefix,
        }
    }
}

impl From<&RedisStringsSinkConfig> for RedisStringsSinkConfigWithoutSecrets {
    fn from(config: &RedisStringsSinkConfig) -> Self {
        Self {
            key_prefix: config.key_prefix.clone(),
        }
    }
}

/// Sink that stores events as Redis string key-value pairs.
///
/// Each event's payload is stored with a dynamic or default key.
/// The sink uses a connection manager for automatic reconnection handling.
#[derive(Clone)]
pub struct RedisStringsSink {
    /// Shared Redis connection manager.
    connection: Arc<Mutex<ConnectionManager>>,

    /// Optional prefix prepended to all keys.
    key_prefix: Option<String>,
}

impl RedisStringsSink {
    /// Creates a new Redis Strings sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis connection cannot be established.
    pub async fn new(
        config: RedisStringsSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = redis::Client::open(config.url)?;
        let connection = ConnectionManager::new(client).await?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            key_prefix: config.key_prefix,
        })
    }

    /// Resolves the Redis key for an event from metadata or default (event ID).
    fn resolve_key(&self, event: &TriggeredEvent) -> String {
        // First check event metadata for dynamic key.
        if let Some(ref metadata) = event.metadata
            && let Some(key) = metadata.get("key").and_then(|v| v.as_str()) {
                return key.to_string();
            }
        // Fall back to event ID with optional prefix.
        match &self.key_prefix {
            Some(prefix) => format!("{}:{}", prefix, event.id.id),
            None => event.id.id.clone(),
        }
    }
}

impl Sink for RedisStringsSink {
    fn name() -> &'static str {
        "redis-strings"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection.lock().await;

        // Use pipeline for batch efficiency.
        let mut pipe = redis::pipe();

        for event in events {
            let key = self.resolve_key(&event);
            pipe.set(&key, event.payload.to_string());
        }

        pipe.query_async::<()>(&mut *conn).await.map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::DestinationError,
                "Failed to publish events to Redis",
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
        assert_eq!(RedisStringsSink::name(), "redis-strings");
    }
}
