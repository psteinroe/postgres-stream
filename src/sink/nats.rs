//! NATS sink for publishing events to a NATS subject.
//!
//! Publishes each event's payload as a JSON message to a subject determined by:
//! 1. `topic` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `subject` in sink config
//!
//! # Dynamic Routing
//!
//! The target subject can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "topic", "expression": "''events.'' || table_name"}
//! ]'
//! ```

use async_nats::Client;
use etl::error::EtlResult;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the NATS sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (URL credentials) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct NatsSinkConfig {
    /// NATS server URL (e.g., "nats://localhost:4222").
    /// Contains credentials and should be treated as sensitive.
    pub url: String,

    /// Subject to publish messages to. Optional if provided via event metadata.
    #[serde(default)]
    pub subject: Option<String>,
}

/// Configuration for the NATS sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NatsSinkConfigWithoutSecrets {
    /// Subject to publish messages to (if configured).
    pub subject: Option<String>,
}

impl From<NatsSinkConfig> for NatsSinkConfigWithoutSecrets {
    fn from(config: NatsSinkConfig) -> Self {
        Self {
            subject: config.subject,
        }
    }
}

impl From<&NatsSinkConfig> for NatsSinkConfigWithoutSecrets {
    fn from(config: &NatsSinkConfig) -> Self {
        Self {
            subject: config.subject.clone(),
        }
    }
}

/// Sink that publishes events to a NATS subject.
///
/// Each event's payload is serialized as JSON and published to the subject.
/// The NATS client handles connection pooling and automatic reconnection.
#[derive(Clone)]
pub struct NatsSink {
    /// Shared NATS client connection.
    client: Arc<Client>,

    /// Default subject to publish messages to. Can be overridden per-event via metadata.
    subject: Option<String>,
}

impl NatsSink {
    /// Creates a new NATS sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the NATS connection cannot be established.
    pub async fn new(
        config: NatsSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = async_nats::connect(&config.url).await?;

        Ok(Self {
            client: Arc::new(client),
            subject: config.subject,
        })
    }

    /// Resolves the subject for an event from metadata or config.
    fn resolve_subject<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic subject (using generic "topic" key).
        if let Some(ref metadata) = event.metadata
            && let Some(topic) = metadata.get("topic").and_then(|v| v.as_str())
        {
            return Some(topic);
        }
        // Fall back to config subject.
        self.subject.as_deref()
    }
}

impl Sink for NatsSink {
    fn name() -> &'static str {
        "nats"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Publish all messages concurrently in a single pass.
        try_join_all(events.into_iter().map(|event| {
            let client = &self.client;
            let subject_opt = self.resolve_subject(&event).map(|s| s.to_string());
            async move {
                let subject = subject_opt.ok_or_else(|| {
                    etl::etl_error!(
                        etl::error::ErrorKind::ConfigError,
                        "No subject configured",
                        "Subject must be provided in sink config or event metadata (topic key)"
                    )
                })?;

                let bytes = serde_json::to_vec(&event.payload).map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::InvalidData,
                        "Failed to serialize payload to JSON",
                        e.to_string()
                    )
                })?;

                client.publish(subject, bytes.into()).await.map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::DestinationError,
                        "Failed to publish event to NATS",
                        e.to_string()
                    )
                })
            }
        }))
        .await?;

        // Flush to ensure all messages are sent.
        self.client.flush().await.map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::DestinationError,
                "Failed to flush NATS messages",
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
        assert_eq!(NatsSink::name(), "nats");
    }
}
