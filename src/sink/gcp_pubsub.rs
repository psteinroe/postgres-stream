//! GCP Pub/Sub sink for publishing events to a Google Cloud Pub/Sub topic.
//!
//! Publishes each event's payload as a message to the configured topic.
//! The sink uses the Google Cloud Pub/Sub client for async message delivery.
//!
//! Note: Unlike other sinks, Pub/Sub requires the topic to be known at sink
//! creation time. Dynamic topic routing via metadata is not supported.
//!
//! # Configuration
//!
//! The topic must be specified in sink config:
//!
//! ```toml
//! [sink.gcp_pubsub]
//! project_id = "my-project"
//! topic = "my-topic"
//! ```

use etl::error::EtlResult;
use futures::future::try_join_all;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the GCP Pub/Sub sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking sensitive information in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct GcpPubsubSinkConfig {
    /// GCP project ID.
    pub project_id: String,

    /// Pub/Sub topic name (not the full path, just the topic name).
    pub topic: String,

    /// Optional emulator host for testing (e.g., "localhost:8085").
    #[serde(default)]
    pub emulator_host: Option<String>,
}

/// Configuration for the GCP Pub/Sub sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GcpPubsubSinkConfigWithoutSecrets {
    /// GCP project ID.
    pub project_id: String,

    /// Pub/Sub topic name.
    pub topic: String,

    /// Whether an emulator host is configured.
    pub uses_emulator: bool,
}

impl From<GcpPubsubSinkConfig> for GcpPubsubSinkConfigWithoutSecrets {
    fn from(config: GcpPubsubSinkConfig) -> Self {
        Self {
            project_id: config.project_id,
            topic: config.topic,
            uses_emulator: config.emulator_host.is_some(),
        }
    }
}

impl From<&GcpPubsubSinkConfig> for GcpPubsubSinkConfigWithoutSecrets {
    fn from(config: &GcpPubsubSinkConfig) -> Self {
        Self {
            project_id: config.project_id.clone(),
            topic: config.topic.clone(),
            uses_emulator: config.emulator_host.is_some(),
        }
    }
}

/// Sink that publishes events to a GCP Pub/Sub topic.
///
/// Each event is serialized as JSON and published as a message.
/// The sink uses the Google Cloud Pub/Sub client for automatic batching.
#[derive(Clone)]
pub struct GcpPubsubSink {
    /// Pub/Sub publisher for the topic.
    publisher: Arc<Publisher>,
}

impl GcpPubsubSink {
    /// Creates a new Pub/Sub sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot be configured or the topic doesn't exist.
    ///
    /// # Emulator Support
    ///
    /// For testing with the Pub/Sub emulator, set the `PUBSUB_EMULATOR_HOST` environment
    /// variable before starting the application (e.g., `PUBSUB_EMULATOR_HOST=localhost:8085`).
    /// The `emulator_host` config option is used only to detect emulator mode for topic
    /// auto-creation, not to set the environment variable.
    pub async fn new(
        config: GcpPubsubSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Build client config.
        let mut client_config = ClientConfig::default();
        client_config.project_id = Some(config.project_id.clone());

        let client = Client::new(client_config).await?;

        // Get the topic.
        let topic = client.topic(&config.topic);

        // Create topic if using emulator (for testing convenience).
        if config.emulator_host.is_some() && !topic.exists(None).await? {
            topic.create(None, None).await?;
        }

        let publisher = topic.new_publisher(None);

        Ok(Self {
            publisher: Arc::new(publisher),
        })
    }
}

impl Sink for GcpPubsubSink {
    fn name() -> &'static str {
        "gcp-pubsub"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Serialize all payloads upfront (fail fast on serialization errors).
        let messages: Vec<_> = events
            .into_iter()
            .map(|event| {
                let data = serde_json::to_vec(&event.payload).map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::InvalidData,
                        "Failed to serialize payload to JSON",
                        e.to_string()
                    )
                })?;

                Ok(google_cloud_googleapis::pubsub::v1::PubsubMessage {
                    data,
                    ..Default::default()
                })
            })
            .collect::<EtlResult<Vec<_>>>()?;

        // Publish all messages concurrently and collect awaiters.
        // The publisher batches messages internally (default: 10ms, 100 messages, or 1MiB).
        let awaiters =
            futures::future::join_all(messages.into_iter().map(|msg| self.publisher.publish(msg)))
                .await;

        // Wait for all messages to be confirmed concurrently.
        try_join_all(awaiters.into_iter().map(|awaiter| async move {
            awaiter.get().await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::DestinationError,
                    "Failed to publish message to Pub/Sub",
                    e.to_string()
                )
            })
        }))
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_name() {
        assert_eq!(GcpPubsubSink::name(), "gcp-pubsub");
    }

    #[test]
    fn test_config_without_secrets() {
        let config = GcpPubsubSinkConfig {
            project_id: "my-project".to_string(),
            topic: "my-topic".to_string(),
            emulator_host: Some("localhost:8085".to_string()),
        };

        let without_secrets: GcpPubsubSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(without_secrets.project_id, "my-project");
        assert_eq!(without_secrets.topic, "my-topic");
        assert!(without_secrets.uses_emulator);
    }
}
