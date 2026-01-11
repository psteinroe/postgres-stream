//! Elasticsearch sink for indexing events as documents.
//!
//! Indexes each event's payload as a JSON document in Elasticsearch.
//! The sink uses bulk indexing for efficient batch operations.
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
//! metadata = '{"index": "events"}'
//! ```
//!
//! Priority: event.metadata["index"] > config.index

use elasticsearch::http::transport::Transport;
use elasticsearch::{BulkOperation, BulkParts, Elasticsearch};
use etl::error::EtlResult;
use serde::{Deserialize, Serialize};

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
    /// Can be overridden per-event via metadata["index"].
    #[serde(default)]
    pub index: Option<String>,
}

/// Configuration for the Elasticsearch sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfigWithoutSecrets {
    /// Elasticsearch URL (may contain credentials, so partially redacted).
    pub url_host: String,

    /// Index name for document storage.
    pub index: Option<String>,
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
            let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
            return format!("{host}{port}");
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

    /// Default index name from config (can be overridden per-event).
    index: Option<String>,
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

impl Sink for ElasticsearchSink {
    fn name() -> &'static str {
        "elasticsearch"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Build bulk operations.
        let mut operations: Vec<BulkOperation<serde_json::Value>> =
            Vec::with_capacity(events.len());

        for event in &events {
            // Resolve target index for this event.
            let index = self.resolve_index(event).ok_or_else(|| {
                etl::etl_error!(
                    etl::error::ErrorKind::ConfigError,
                    "No index in config or event metadata"
                )
            })?;

            // Index only the payload (not full event envelope).
            let op = BulkOperation::index(event.payload.clone())
                .id(&event.id.id)
                .index(index)
                .into();
            operations.push(op);
        }

        // Execute bulk request (no default index needed since each op has its own).
        let response = self
            .client
            .bulk(BulkParts::None)
            .body(operations)
            .send()
            .await
            .map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::DestinationError,
                    "Failed to execute bulk request",
                    e.to_string()
                )
            })?;

        // Check response status.
        if !response.status_code().is_success() {
            let status = response.status_code();
            return Err(etl::etl_error!(
                etl::error::ErrorKind::DestinationError,
                "Elasticsearch bulk request failed",
                format!("status: {}", status)
            ));
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
            index: Some("events".to_string()),
        };

        let without_secrets: ElasticsearchSinkConfigWithoutSecrets = (&config).into();

        // Should extract only host:port, no credentials.
        assert_eq!(without_secrets.url_host, "localhost:9200");
        assert_eq!(without_secrets.index, Some("events".to_string()));
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
        assert_eq!(
            extract_host("http://elasticsearch.local"),
            "elasticsearch.local"
        );
    }
}
