//! RabbitMQ sink for publishing events to a message queue.
//!
//! Publishes each event's payload as a JSON message to an exchange determined by:
//! 1. `exchange` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `exchange` in sink config
//!
//! The routing key is determined by:
//! 1. `routing_key` key in event metadata
//! 2. Fallback to `routing_key` in sink config
//!
//! # Dynamic Routing
//!
//! The target exchange and routing key can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "exchange", "expression": "''events''"},
//!   {"json_path": "routing_key", "expression": "table_name || ''.'' || operation"}
//! ]'
//! ```

use etl::error::EtlResult;
use futures::future::try_join_all;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the RabbitMQ sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (URL credentials) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct RabbitmqSinkConfig {
    /// RabbitMQ connection URL (e.g., "amqp://guest:guest@localhost:5672").
    /// Contains credentials and should be treated as sensitive.
    pub url: String,

    /// Exchange name to publish messages to. Optional if provided via event metadata.
    #[serde(default)]
    pub exchange: Option<String>,

    /// Routing key for message routing. Optional if provided via event metadata.
    #[serde(default)]
    pub routing_key: Option<String>,

    /// Optional queue name to bind to the exchange.
    /// If provided, the queue will be declared and bound.
    #[serde(default)]
    pub queue: Option<String>,
}

/// Configuration for the RabbitMQ sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RabbitmqSinkConfigWithoutSecrets {
    /// Exchange name to publish messages to (if configured).
    pub exchange: Option<String>,

    /// Routing key for message routing (if configured).
    pub routing_key: Option<String>,

    /// Optional queue name to bind to the exchange.
    pub queue: Option<String>,
}

impl From<RabbitmqSinkConfig> for RabbitmqSinkConfigWithoutSecrets {
    fn from(config: RabbitmqSinkConfig) -> Self {
        Self {
            exchange: config.exchange,
            routing_key: config.routing_key,
            queue: config.queue,
        }
    }
}

impl From<&RabbitmqSinkConfig> for RabbitmqSinkConfigWithoutSecrets {
    fn from(config: &RabbitmqSinkConfig) -> Self {
        Self {
            exchange: config.exchange.clone(),
            routing_key: config.routing_key.clone(),
            queue: config.queue.clone(),
        }
    }
}

/// Sink that publishes events to a RabbitMQ exchange.
///
/// Each event's payload is serialized as JSON and published to the exchange.
/// The sink maintains a persistent connection and channel for publishing.
#[derive(Clone)]
pub struct RabbitmqSink {
    /// Shared channel for publishing messages.
    channel: Arc<Mutex<Channel>>,

    /// Default exchange name. Can be overridden per-event via metadata.
    exchange: Option<String>,

    /// Default routing key. Can be overridden per-event via metadata.
    routing_key: Option<String>,
}

impl RabbitmqSink {
    /// Creates a new RabbitMQ sink from configuration.
    ///
    /// Establishes a connection, creates a channel, and optionally declares
    /// an exchange and queue with bindings.
    ///
    /// # Errors
    ///
    /// Returns an error if the RabbitMQ connection cannot be established.
    pub async fn new(
        config: RabbitmqSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let connection = Connection::connect(&config.url, ConnectionProperties::default()).await?;

        let channel = connection.create_channel().await?;

        // Declare the exchange as a topic exchange if provided.
        if let Some(ref exchange) = config.exchange {
            channel
                .exchange_declare(
                    exchange,
                    lapin::ExchangeKind::Topic,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await?;

            // If a queue is specified, declare and bind it.
            if let Some(ref queue_name) = config.queue {
                if let Some(ref routing_key) = config.routing_key {
                    channel
                        .queue_declare(
                            queue_name,
                            QueueDeclareOptions {
                                durable: true,
                                ..Default::default()
                            },
                            FieldTable::default(),
                        )
                        .await?;

                    channel
                        .queue_bind(
                            queue_name,
                            exchange,
                            routing_key,
                            QueueBindOptions::default(),
                            FieldTable::default(),
                        )
                        .await?;
                }
            }
        }

        Ok(Self {
            channel: Arc::new(Mutex::new(channel)),
            exchange: config.exchange,
            routing_key: config.routing_key,
        })
    }

    /// Resolves the exchange name for an event from metadata or config.
    fn resolve_exchange<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic exchange.
        if let Some(ref metadata) = event.metadata {
            if let Some(exchange) = metadata.get("exchange").and_then(|v| v.as_str()) {
                return Some(exchange);
            }
        }
        // Fall back to config exchange.
        self.exchange.as_deref()
    }

    /// Resolves the routing key for an event from metadata or config.
    fn resolve_routing_key<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic routing key.
        if let Some(ref metadata) = event.metadata {
            if let Some(routing_key) = metadata.get("routing_key").and_then(|v| v.as_str()) {
                return Some(routing_key);
            }
        }
        // Fall back to config routing key.
        self.routing_key.as_deref()
    }
}

impl Sink for RabbitmqSink {
    fn name() -> &'static str {
        "rabbitmq"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Validate and serialize payloads in a single pass.
        // Keep events alive to borrow exchange/routing_key without allocating.
        let payloads: Vec<Vec<u8>> = events
            .iter()
            .map(|event| {
                // Validate config errors while we iterate.
                self.resolve_exchange(event).ok_or_else(|| {
                    etl::etl_error!(
                        etl::error::ErrorKind::ConfigError,
                        "No exchange configured",
                        "Exchange must be provided in sink config or event metadata"
                    )
                })?;
                self.resolve_routing_key(event).ok_or_else(|| {
                    etl::etl_error!(
                        etl::error::ErrorKind::ConfigError,
                        "No routing_key configured",
                        "Routing key must be provided in sink config or event metadata"
                    )
                })?;

                serde_json::to_vec(&event.payload).map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::InvalidData,
                        "Failed to serialize payload to JSON",
                        e.to_string()
                    )
                })
            })
            .collect::<EtlResult<_>>()?;

        // Publish all messages concurrently, borrowing exchange/routing_key from events.
        let channel = self.channel.lock().await;
        let confirms = try_join_all(events.iter().zip(&payloads).map(|(event, payload)| {
            channel.basic_publish(
                self.resolve_exchange(event).unwrap(), // Already validated.
                self.resolve_routing_key(event).unwrap(),
                BasicPublishOptions::default(),
                payload,
                BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // Persistent.
            )
        }))
        .await
        .map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::DestinationError,
                "Failed to publish event to RabbitMQ",
                e.to_string()
            )
        })?;
        drop(channel);

        // Await all confirms concurrently.
        try_join_all(confirms.into_iter().map(|confirm| async move {
            confirm.await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::DestinationError,
                    "Failed to confirm RabbitMQ publish",
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
        assert_eq!(RabbitmqSink::name(), "rabbitmq");
    }
}
