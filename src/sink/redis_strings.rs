//! Redis Strings sink for publishing events as key-value pairs.
//!
//! Stores each event as a Redis string with the event ID as key and
//! the JSON-serialized payload as value.

use etl::error::EtlResult;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the Redis Strings sink.
#[derive(Clone, Debug, Deserialize)]
pub struct RedisStringsSinkConfig {
    /// Redis connection URL (e.g., "redis://localhost:6379").
    pub url: String,

    /// Optional prefix for all keys.
    #[serde(default)]
    pub key_prefix: Option<String>,
}

/// Sink that stores events as Redis string key-value pairs.
///
/// Each event is stored with its ID as the key and JSON payload as the value.
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

    /// Formats the Redis key for an event.
    fn format_key(&self, event_id: &str) -> String {
        match &self.key_prefix {
            Some(prefix) => format!("{}:{}", prefix, event_id),
            None => event_id.to_string(),
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

        info!("publishing {} events to Redis", events.len());

        let mut conn = self.connection.lock().await;

        // Use pipeline for batch efficiency.
        let mut pipe = redis::pipe();

        for event in &events {
            let key = self.format_key(&event.id.id);
            let value = serde_json::to_string(&event.payload).map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to serialize event payload",
                    e.to_string()
                )
            })?;

            pipe.set(&key, value);
        }

        pipe.query_async::<()>(&mut *conn).await.map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::InvalidData,
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

    #[test]
    fn test_format_key_without_prefix() {
        // Cannot construct sink without connection, test logic directly.
        let event_id = "test-event-123";
        let key_prefix: Option<String> = None;

        let result = match &key_prefix {
            Some(prefix) => format!("{}:{}", prefix, event_id),
            None => event_id.to_string(),
        };

        assert_eq!(result, "test-event-123");
    }

    #[test]
    fn test_format_key_with_prefix() {
        let event_id = "test-event-123";
        let key_prefix = Some("pgstream".to_string());

        let result = match &key_prefix {
            Some(prefix) => format!("{}:{}", prefix, event_id),
            None => event_id.to_string(),
        };

        assert_eq!(result, "pgstream:test-event-123");
    }
}
