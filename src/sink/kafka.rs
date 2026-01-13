//! Kafka sink for publishing events to a Kafka topic.
//!
//! Publishes each event's payload as a JSON message to a topic determined by:
//! 1. `topic` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `topic` in sink config
//!
//! # Dynamic Routing
//!
//! The target topic can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "topic", "expression": "''events-'' || table_name"}
//! ]'
//! ```

use etl::error::EtlResult;
use futures::future::try_join_all;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Default delivery timeout of 5 seconds.
const DEFAULT_DELIVERY_TIMEOUT_MS: u64 = 5000;

/// Configuration for the Kafka sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (broker credentials, SASL passwords) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct KafkaSinkConfig {
    /// Comma-separated list of Kafka brokers (e.g., "localhost:9092").
    /// May contain credentials and should be treated as sensitive.
    pub brokers: String,

    /// Topic to produce messages to. Optional if provided via event metadata.
    #[serde(default)]
    pub topic: Option<String>,

    /// Optional SASL username for authentication.
    #[serde(default)]
    pub sasl_username: Option<String>,

    /// Optional SASL password for authentication.
    #[serde(default)]
    pub sasl_password: Option<String>,

    /// Optional SASL mechanism (e.g., "PLAIN", "SCRAM-SHA-256").
    #[serde(default)]
    pub sasl_mechanism: Option<String>,

    /// Optional security protocol (e.g., "SASL_SSL", "SASL_PLAINTEXT").
    #[serde(default)]
    pub security_protocol: Option<String>,

    /// Message delivery timeout in milliseconds (default: 5000).
    #[serde(default = "default_delivery_timeout_ms")]
    pub delivery_timeout_ms: u64,
}

/// Default delivery timeout for serde.
fn default_delivery_timeout_ms() -> u64 {
    DEFAULT_DELIVERY_TIMEOUT_MS
}

/// Configuration for the Kafka sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSinkConfigWithoutSecrets {
    /// Topic to produce messages to (if configured).
    pub topic: Option<String>,

    /// Message delivery timeout in milliseconds.
    pub delivery_timeout_ms: u64,

    /// Whether SASL authentication is configured.
    pub sasl_enabled: bool,

    /// Security protocol in use, if any.
    pub security_protocol: Option<String>,
}

impl From<KafkaSinkConfig> for KafkaSinkConfigWithoutSecrets {
    fn from(config: KafkaSinkConfig) -> Self {
        Self {
            topic: config.topic,
            delivery_timeout_ms: config.delivery_timeout_ms,
            sasl_enabled: config.sasl_username.is_some(),
            security_protocol: config.security_protocol,
        }
    }
}

impl From<&KafkaSinkConfig> for KafkaSinkConfigWithoutSecrets {
    fn from(config: &KafkaSinkConfig) -> Self {
        Self {
            topic: config.topic.clone(),
            delivery_timeout_ms: config.delivery_timeout_ms,
            sasl_enabled: config.sasl_username.is_some(),
            security_protocol: config.security_protocol.clone(),
        }
    }
}

/// Sink that produces events to a Kafka topic.
///
/// Each event's payload is serialized as JSON and produced to the topic.
/// The sink uses librdkafka's FutureProducer for async message delivery.
#[derive(Clone)]
pub struct KafkaSink {
    /// Kafka producer for async message delivery.
    producer: Arc<FutureProducer>,

    /// Default topic to produce messages to. Can be overridden per-event via metadata.
    topic: Option<String>,

    /// Delivery timeout for message production.
    delivery_timeout: Duration,
}

impl KafkaSink {
    /// Creates a new Kafka sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Kafka producer cannot be created.
    pub fn new(config: KafkaSinkConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);

        // Configure SASL authentication if provided.
        if let Some(ref mechanism) = config.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref username) = config.sasl_username {
            client_config.set("sasl.username", username);
        }
        if let Some(ref password) = config.sasl_password {
            client_config.set("sasl.password", password);
        }
        if let Some(ref protocol) = config.security_protocol {
            client_config.set("security.protocol", protocol);
        }

        // Set message timeout.
        client_config.set("message.timeout.ms", config.delivery_timeout_ms.to_string());

        let producer: FutureProducer = client_config.create()?;

        Ok(Self {
            producer: Arc::new(producer),
            topic: config.topic,
            delivery_timeout: Duration::from_millis(config.delivery_timeout_ms),
        })
    }

    /// Resolves the topic for an event from metadata or config.
    fn resolve_topic<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic topic.
        if let Some(ref metadata) = event.metadata {
            if let Some(topic) = metadata.get("topic").and_then(|v| v.as_str()) {
                return Some(topic);
            }
        }
        // Fall back to config topic.
        self.topic.as_deref()
    }
}

impl Sink for KafkaSink {
    fn name() -> &'static str {
        "kafka"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Send all messages concurrently in a single pass.
        try_join_all(events.into_iter().map(|event| {
            let producer = &self.producer;
            let topic_opt = self.resolve_topic(&event).map(|s| s.to_string());
            let timeout = self.delivery_timeout;

            async move {
                let topic = topic_opt.ok_or_else(|| {
                    etl::etl_error!(
                        etl::error::ErrorKind::ConfigError,
                        "No topic configured",
                        "Topic must be provided in sink config or event metadata"
                    )
                })?;

                let payload = serde_json::to_vec(&event.payload).map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::InvalidData,
                        "Failed to serialize payload to JSON",
                        e.to_string()
                    )
                })?;

                let record = FutureRecord::to(&topic)
                    .key(event.id.id.as_str())
                    .payload(payload.as_slice());

                producer.send(record, timeout).await.map_err(|(e, _)| {
                    etl::etl_error!(
                        etl::error::ErrorKind::DestinationError,
                        "Failed to produce message to Kafka",
                        e.to_string()
                    )
                })?;
                Ok::<_, etl::error::EtlError>(())
            }
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
        assert_eq!(KafkaSink::name(), "kafka");
    }

    #[test]
    fn test_default_delivery_timeout() {
        assert_eq!(DEFAULT_DELIVERY_TIMEOUT_MS, 5000);
    }

    #[test]
    fn test_config_without_secrets() {
        let config = KafkaSinkConfig {
            brokers: "localhost:9092".to_string(),
            topic: Some("test-topic".to_string()),
            sasl_username: Some("user".to_string()),
            sasl_password: Some("secret".to_string()),
            sasl_mechanism: Some("PLAIN".to_string()),
            security_protocol: Some("SASL_PLAINTEXT".to_string()),
            delivery_timeout_ms: 10000,
        };

        let without_secrets: KafkaSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(without_secrets.topic, Some("test-topic".to_string()));
        assert_eq!(without_secrets.delivery_timeout_ms, 10000);
        assert!(without_secrets.sasl_enabled);
        assert_eq!(
            without_secrets.security_protocol,
            Some("SASL_PLAINTEXT".to_string())
        );
    }
}
