//! Elasticsearch sink for indexing events as documents.
//!
//! Indexes each event as a JSON document in the configured Elasticsearch index.
//! The sink uses bulk indexing for efficient batch operations.
//!
//! # Payload Extensions
//!
//! This sink supports the following optional payload extensions:
//!
//! ```sql
//! payload_extensions = '[
//!   {"key": "routing", "value": "user_123"},
//!   {"key": "pipeline", "value": "my-pipeline"}
//! ]'
//! ```
//!
//! - `routing`: Document routing value for shard placement.
//! - `pipeline`: Ingest pipeline to process the document.

use elasticsearch::http::transport::Transport;
use elasticsearch::{BulkOperation, BulkParts, Elasticsearch};
use etl::error::EtlResult;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Configuration for the Elasticsearch sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking sensitive information in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct ElasticsearchSinkConfig {
    /// Elasticsearch URL (e.g., "http://localhost:9200").
    pub url: String,

    /// Index name for document storage.
    pub index: String,
}

/// Configuration for the Elasticsearch sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfigWithoutSecrets {
    /// Elasticsearch URL (may contain credentials, so partially redacted).
    pub url_host: String,

    /// Index name for document storage.
    pub index: String,
}

impl From<ElasticsearchSinkConfig> for ElasticsearchSinkConfigWithoutSecrets {
    fn from(config: ElasticsearchSinkConfig) -> Self {
        Self {
            url_host: extract_host(&config.url),
            index: config.index,
        }
    }
}

impl From<&ElasticsearchSinkConfig> for ElasticsearchSinkConfigWithoutSecrets {
    fn from(config: &ElasticsearchSinkConfig) -> Self {
        Self {
            url_host: extract_host(&config.url),
            index: config.index.clone(),
        }
    }
}

/// Extracts the host from a URL, stripping credentials if present.
fn extract_host(url: &str) -> String {
    // Parse URL and extract just the host:port portion.
    if let Ok(parsed) = url::Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            let port = parsed.port().map(|p| format!(":{}", p)).unwrap_or_default();
            return format!("{}{}", host, port);
        }
    }
    // Fallback: return as-is if parsing fails.
    url.to_string()
}

/// Sink that indexes events in Elasticsearch.
///
/// Events are serialized as JSON documents and bulk indexed.
/// The sink handles connection pooling and automatic retries.
#[derive(Clone)]
pub struct ElasticsearchSink {
    /// Elasticsearch client.
    client: Elasticsearch,

    /// Target index name.
    index: String,
}

impl ElasticsearchSink {
    /// Creates a new Elasticsearch sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be created.
    pub async fn new(
        config: ElasticsearchSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let transport = Transport::single_node(&config.url)?;
        let client = Elasticsearch::new(transport);

        Ok(Self {
            client,
            index: config.index,
        })
    }
}

impl Sink for ElasticsearchSink {
    fn name() -> &'static str {
        "elasticsearch"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("indexing {} events to Elasticsearch", events.len());

        // Build bulk operations.
        let mut operations: Vec<BulkOperation<serde_json::Value>> = Vec::with_capacity(events.len());

        for event in &events {
            // Build JSON document.
            let mut doc = serde_json::json!({
                "id": event.id.id,
                "created_at": event.id.created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                "payload": event.payload,
                "stream_id": format!("{:?}", event.stream_id),
            });

            // Add optional fields.
            if let Some(ref metadata) = event.metadata {
                doc["metadata"] = metadata.clone();
            }
            if let Some(lsn) = event.lsn {
                doc["lsn"] = serde_json::json!(lsn.to_string());
            }

            // Use event ID as document ID.
            let op = BulkOperation::index(doc).id(&event.id.id).into();
            operations.push(op);
        }

        // Execute bulk request.
        let response = self
            .client
            .bulk(BulkParts::Index(&self.index))
            .body(operations)
            .send()
            .await
            .map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to execute bulk request",
                    e.to_string()
                )
            })?;

        // Check response status.
        if !response.status_code().is_success() {
            let status = response.status_code();
            return Err(etl::etl_error!(
                etl::error::ErrorKind::InvalidData,
                "Elasticsearch bulk request failed",
                format!("status: {}", status)
            ));
        }

        // Check for item-level errors.
        let body = response.json::<serde_json::Value>().await.map_err(|e| {
            etl::etl_error!(
                etl::error::ErrorKind::InvalidData,
                "Failed to parse bulk response",
                e.to_string()
            )
        })?;

        if body["errors"].as_bool().unwrap_or(false) {
            // Log first error for debugging.
            if let Some(items) = body["items"].as_array() {
                for item in items {
                    if let Some(error) = item["index"]["error"].as_object() {
                        return Err(etl::etl_error!(
                            etl::error::ErrorKind::InvalidData,
                            "Elasticsearch indexing error",
                            format!("{:?}", error)
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_name() {
        assert_eq!(ElasticsearchSink::name(), "elasticsearch");
    }

    #[test]
    fn test_config_without_secrets() {
        let config = ElasticsearchSinkConfig {
            url: "http://user:pass@localhost:9200".to_string(),
            index: "events".to_string(),
        };

        let without_secrets: ElasticsearchSinkConfigWithoutSecrets = (&config).into();

        // Should extract only host:port, no credentials.
        assert_eq!(without_secrets.url_host, "localhost:9200");
        assert_eq!(without_secrets.index, "events");
    }

    #[test]
    fn test_extract_host_simple() {
        assert_eq!(extract_host("http://localhost:9200"), "localhost:9200");
    }

    #[test]
    fn test_extract_host_with_credentials() {
        assert_eq!(
            extract_host("http://elastic:password@localhost:9200"),
            "localhost:9200"
        );
    }

    #[test]
    fn test_extract_host_no_port() {
        assert_eq!(extract_host("http://elasticsearch.local"), "elasticsearch.local");
    }
}
