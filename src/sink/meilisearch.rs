//! Meilisearch sink for indexing events as searchable documents.
//!
//! Indexes each event as a JSON document in the configured Meilisearch index.
//! The sink uses bulk document addition for efficient batch operations.
//!
//! # Payload Extensions
//!
//! This sink does not currently support any payload extensions.
//! Future versions may add support for custom ranking or filtering attributes.

use etl::error::EtlResult;
use meilisearch_sdk::client::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the Meilisearch sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking sensitive information in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct MeilisearchSinkConfig {
    /// Meilisearch URL (e.g., "http://localhost:7700").
    pub url: String,

    /// Index name for document storage.
    pub index: String,

    /// Optional API key for authentication.
    #[serde(default)]
    pub api_key: Option<String>,
}

/// Configuration for the Meilisearch sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeilisearchSinkConfigWithoutSecrets {
    /// Meilisearch URL.
    pub url: String,

    /// Index name for document storage.
    pub index: String,

    /// Whether an API key is configured.
    pub has_api_key: bool,
}

impl From<MeilisearchSinkConfig> for MeilisearchSinkConfigWithoutSecrets {
    fn from(config: MeilisearchSinkConfig) -> Self {
        Self {
            url: config.url,
            index: config.index,
            has_api_key: config.api_key.is_some(),
        }
    }
}

impl From<&MeilisearchSinkConfig> for MeilisearchSinkConfigWithoutSecrets {
    fn from(config: &MeilisearchSinkConfig) -> Self {
        Self {
            url: config.url.clone(),
            index: config.index.clone(),
            has_api_key: config.api_key.is_some(),
        }
    }
}

/// Document structure for Meilisearch indexing.
///
/// This struct is used internally for serialization.
#[derive(Serialize)]
struct MeilisearchDocument {
    /// Unique document identifier (event ID).
    id: String,

    /// Event creation timestamp in RFC3339 format.
    created_at: String,

    /// Event payload data.
    payload: serde_json::Value,

    /// Stream identifier.
    stream_id: String,

    /// Optional event metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,

    /// Optional log sequence number.
    #[serde(skip_serializing_if = "Option::is_none")]
    lsn: Option<String>,
}

/// Sink that indexes events in Meilisearch.
///
/// Events are serialized as JSON documents and batch indexed.
/// The sink handles connection management and task waiting.
#[derive(Clone)]
pub struct MeilisearchSink {
    /// Meilisearch client.
    client: Arc<Client>,

    /// Target index name.
    index: String,
}

impl MeilisearchSink {
    /// Creates a new Meilisearch sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot be created.
    pub async fn new(
        config: MeilisearchSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::new(&config.url, config.api_key.as_deref())?;

        Ok(Self {
            client: Arc::new(client),
            index: config.index,
        })
    }
}

impl Sink for MeilisearchSink {
    fn name() -> &'static str {
        "meilisearch"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("indexing {} events to Meilisearch", events.len());

        // Build documents for indexing.
        let documents: Vec<MeilisearchDocument> = events
            .iter()
            .map(|event| MeilisearchDocument {
                id: event.id.id.clone(),
                created_at: event
                    .id
                    .created_at
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                payload: event.payload.clone(),
                stream_id: format!("{:?}", event.stream_id),
                metadata: event.metadata.clone(),
                lsn: event.lsn.map(|l| l.to_string()),
            })
            .collect();

        // Get or create the index and add documents.
        let index = self.client.index(&self.index);

        let task = index
            .add_documents(&documents, Some("id"))
            .await
            .map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to add documents to Meilisearch",
                    e.to_string()
                )
            })?;

        // Wait for the task to complete.
        task.wait_for_completion(&self.client, None, None)
            .await
            .map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to wait for Meilisearch task",
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
        assert_eq!(MeilisearchSink::name(), "meilisearch");
    }

    #[test]
    fn test_config_without_secrets() {
        let config = MeilisearchSinkConfig {
            url: "http://localhost:7700".to_string(),
            index: "events".to_string(),
            api_key: Some("secret-api-key".to_string()),
        };

        let without_secrets: MeilisearchSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(without_secrets.url, "http://localhost:7700");
        assert_eq!(without_secrets.index, "events");
        assert!(without_secrets.has_api_key);
    }

    #[test]
    fn test_config_without_secrets_no_api_key() {
        let config = MeilisearchSinkConfig {
            url: "http://localhost:7700".to_string(),
            index: "events".to_string(),
            api_key: None,
        };

        let without_secrets: MeilisearchSinkConfigWithoutSecrets = (&config).into();

        assert!(!without_secrets.has_api_key);
    }
}
