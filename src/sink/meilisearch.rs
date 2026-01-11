//! Meilisearch sink for indexing events as searchable documents.
//!
//! Indexes each event's payload as a JSON document in the configured Meilisearch index.
//! The sink uses bulk document addition for efficient batch operations.
//!
//! # Dynamic Routing
//!
//! The target index can come from event metadata or sink config:
//!
//! ```sql
//! -- Via metadata_extensions (dynamic per-event)
//! metadata_extensions = '[{"json_path": "index", "expression": "new.index_name"}]'
//!
//! -- Via static metadata
//! metadata = '{"index": "products"}'
//! ```
//!
//! Priority: event.metadata["index"] > config.index
//!
//! # Primary Key
//!
//! Meilisearch requires each document to have a primary key. Configure the primary
//! key field name in your Meilisearch index settings, or use `payload_extensions`
//! to add/transform the primary key field before sending.

use std::collections::HashMap;
use std::sync::Arc;

use etl::error::EtlResult;
use futures::future::try_join_all;
use meilisearch_sdk::client::Client;
use serde::{Deserialize, Serialize};

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
    /// Can be overridden per-event via metadata["index"].
    #[serde(default)]
    pub index: Option<String>,

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
    pub index: Option<String>,

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

/// Sink that indexes events in Meilisearch.
///
/// Events are serialized as JSON documents and batch indexed.
/// The sink handles connection management and task waiting.
#[derive(Clone)]
pub struct MeilisearchSink {
    /// Meilisearch client.
    client: Arc<Client>,

    /// Default index name from config (can be overridden per-event).
    index: Option<String>,
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

    /// Resolves the index name for an event.
    ///
    /// Priority: event.metadata["index"] > config.index
    fn resolve_index<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // Check event metadata first.
        if let Some(ref metadata) = event.metadata {
            if let Some(index) = metadata.get("index").and_then(|v| v.as_str()) {
                return Some(index);
            }
        }
        // Fall back to config.
        self.index.as_deref()
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

        // Group events by target index (supports per-event routing).
        let mut index_documents: HashMap<String, Vec<serde_json::Value>> = HashMap::new();

        for event in &events {
            let index_name = self.resolve_index(event).ok_or_else(|| {
                etl::etl_error!(
                    etl::error::ErrorKind::ConfigError,
                    "No index in config or event metadata"
                )
            })?;

            index_documents
                .entry(index_name.to_string())
                .or_default()
                .push(event.payload.clone());
        }

        // Index documents to all target indexes concurrently.
        let futures: Vec<_> = index_documents
            .into_iter()
            .map(|(index_name, documents)| {
                let client = self.client.clone();
                async move {
                    let index = client.index(&index_name);

                    let task = index.add_documents(&documents, None).await.map_err(|e| {
                        etl::etl_error!(
                            etl::error::ErrorKind::DestinationError,
                            "Failed to add documents to Meilisearch",
                            e.to_string()
                        )
                    })?;

                    // Wait for the task to complete.
                    task.wait_for_completion(&client, None, None)
                        .await
                        .map_err(|e| {
                            etl::etl_error!(
                                etl::error::ErrorKind::DestinationError,
                                "Failed to wait for Meilisearch task",
                                e.to_string()
                            )
                        })?;

                    Ok::<_, etl::error::EtlError>(())
                }
            })
            .collect();

        try_join_all(futures).await?;

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
            index: Some("events".to_string()),
            api_key: Some("secret-api-key".to_string()),
        };

        let without_secrets: MeilisearchSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(without_secrets.url, "http://localhost:7700");
        assert_eq!(without_secrets.index, Some("events".to_string()));
        assert!(without_secrets.has_api_key);
    }

    #[test]
    fn test_config_without_secrets_no_api_key() {
        let config = MeilisearchSinkConfig {
            url: "http://localhost:7700".to_string(),
            index: Some("events".to_string()),
            api_key: None,
        };

        let without_secrets: MeilisearchSinkConfigWithoutSecrets = (&config).into();

        assert!(!without_secrets.has_api_key);
    }
}
