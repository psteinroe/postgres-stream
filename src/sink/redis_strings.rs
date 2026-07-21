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

use crate::sink::{Sink, redis_common};
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

    /// Timeout for establishing a Redis connection, in milliseconds.
    #[serde(default)]
    pub connection_timeout_ms: Option<u64>,

    /// Timeout for Redis command responses, in milliseconds.
    #[serde(default)]
    pub response_timeout_ms: Option<u64>,

    /// Number of reconnection attempts made by the connection manager.
    #[serde(default)]
    pub connection_retries: Option<usize>,

    /// Maximum delay between reconnection attempts, in milliseconds.
    #[serde(default)]
    pub connection_max_delay_ms: Option<u64>,
}

/// Configuration for the Redis Strings sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisStringsSinkConfigWithoutSecrets {
    /// Optional prefix for all keys.
    pub key_prefix: Option<String>,

    /// Timeout for establishing a Redis connection, in milliseconds.
    pub connection_timeout_ms: Option<u64>,

    /// Timeout for Redis command responses, in milliseconds.
    pub response_timeout_ms: Option<u64>,

    /// Number of reconnection attempts made by the connection manager.
    pub connection_retries: Option<usize>,

    /// Maximum delay between reconnection attempts, in milliseconds.
    pub connection_max_delay_ms: Option<u64>,
}

impl From<RedisStringsSinkConfig> for RedisStringsSinkConfigWithoutSecrets {
    fn from(config: RedisStringsSinkConfig) -> Self {
        Self {
            key_prefix: config.key_prefix,
            connection_timeout_ms: config.connection_timeout_ms,
            response_timeout_ms: config.response_timeout_ms,
            connection_retries: config.connection_retries,
            connection_max_delay_ms: config.connection_max_delay_ms,
        }
    }
}

impl From<&RedisStringsSinkConfig> for RedisStringsSinkConfigWithoutSecrets {
    fn from(config: &RedisStringsSinkConfig) -> Self {
        Self {
            key_prefix: config.key_prefix.clone(),
            connection_timeout_ms: config.connection_timeout_ms,
            response_timeout_ms: config.response_timeout_ms,
            connection_retries: config.connection_retries,
            connection_max_delay_ms: config.connection_max_delay_ms,
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
    pub async fn new(config: RedisStringsSinkConfig) -> EtlResult<Self> {
        let settings = redis_common::RedisConnectionSettings {
            connection_timeout_ms: config.connection_timeout_ms,
            response_timeout_ms: config.response_timeout_ms,
            connection_retries: config.connection_retries,
            connection_max_delay_ms: config.connection_max_delay_ms,
        };
        let connection = redis_common::connect(config.url, settings)
            .await
            .map_err(|error| {
                redis_common::map_error(error, "failed to create Redis Strings sink")
            })?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            key_prefix: config.key_prefix,
        })
    }

    /// Resolves the Redis key for an event from metadata or default (event ID).
    fn resolve_key(&self, event: &TriggeredEvent) -> String {
        // First check event metadata for dynamic key.
        if let Some(ref metadata) = event.metadata
            && let Some(key) = metadata.get("key").and_then(|v| v.as_str())
        {
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

        pipe.query_async::<()>(&mut *conn)
            .await
            .map_err(|error| redis_common::map_error(error, "failed to publish events to Redis"))?;

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

    #[test]
    fn test_connection_settings_deserialize_and_remain_safe_to_serialize() {
        let config: RedisStringsSinkConfig = serde_yaml::from_str(
            r#"
url: redis://user:secret@localhost:6379
key_prefix: events
connection_timeout_ms: 1000
response_timeout_ms: 5000
connection_retries: 2
connection_max_delay_ms: 750
"#,
        )
        .unwrap();

        assert_eq!(config.connection_timeout_ms, Some(1000));
        assert_eq!(config.response_timeout_ms, Some(5000));
        assert_eq!(config.connection_retries, Some(2));
        assert_eq!(config.connection_max_delay_ms, Some(750));

        let serialized =
            serde_yaml::to_string(&RedisStringsSinkConfigWithoutSecrets::from(&config)).unwrap();
        assert!(!serialized.contains("secret"));
        assert!(serialized.contains("response_timeout_ms: 5000"));
    }
}
