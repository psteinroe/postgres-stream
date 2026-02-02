//! Webhook sink for publishing events via HTTP POST requests.
//!
//! Events are grouped by URL and sent as a single request with an array of payloads.
//! The target URL is determined by:
//! 1. `url` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `url` in sink config
//!
//! Supports custom headers from both config and event metadata.
//!
//! # Dynamic Routing
//!
//! The target URL can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "url", "expression": "''https://api.example.com/'' || tenant_id"}
//! ]'
//! ```
//!
//! Additional headers can also be set dynamically:
//!
//! ```sql
//! metadata = '{"headers": {"X-Tenant-Id": "tenant-123"}}'
//! ```

use etl::error::EtlResult;
use futures::future::try_join_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Default timeout of 30 seconds.
const DEFAULT_TIMEOUT_MS: u64 = 30000;

/// Configuration for the Webhook sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (URL with auth tokens, headers with API keys) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct WebhookSinkConfig {
    /// URL to POST events to. Optional if provided via event metadata.
    /// May contain authentication tokens and should be treated as sensitive.
    pub url: Option<String>,

    /// Optional custom headers to include in requests.
    /// May contain API keys or auth tokens and should be treated as sensitive.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Request timeout in milliseconds (default: 30000).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Returns the default timeout value.
fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

/// Configuration for the Webhook sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookSinkConfigWithoutSecrets {
    /// Request timeout in milliseconds.
    pub timeout_ms: u64,

    /// Number of custom headers configured (without revealing their contents).
    pub header_count: usize,
}

impl From<WebhookSinkConfig> for WebhookSinkConfigWithoutSecrets {
    fn from(config: WebhookSinkConfig) -> Self {
        Self {
            timeout_ms: config.timeout_ms,
            header_count: config.headers.len(),
        }
    }
}

impl From<&WebhookSinkConfig> for WebhookSinkConfigWithoutSecrets {
    fn from(config: &WebhookSinkConfig) -> Self {
        Self {
            timeout_ms: config.timeout_ms,
            header_count: config.headers.len(),
        }
    }
}

/// Sink that publishes events via HTTP POST requests.
///
/// Each event's payload is sent as JSON to the configured URL.
/// The sink uses a persistent HTTP client with connection pooling.
#[derive(Clone)]
pub struct WebhookSink {
    /// Shared HTTP client for connection pooling.
    client: Arc<Client>,

    /// Default URL to POST events to. Can be overridden per-event via metadata.
    url: Option<String>,

    /// Custom headers to include in requests.
    headers: HashMap<String, String>,
}

impl WebhookSink {
    /// Creates a new Webhook sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    pub fn new(
        config: WebhookSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()?;

        Ok(Self {
            client: Arc::new(client),
            url: config.url,
            headers: config.headers,
        })
    }

    /// Resolves the URL for an event from metadata or config.
    fn resolve_url<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic URL
        if let Some(ref metadata) = event.metadata
            && let Some(url) = metadata.get("url").and_then(|v| v.as_str()) {
                return Some(url);
            }
        // Fall back to config URL
        self.url.as_deref()
    }

    /// Merges headers from config and event metadata.
    fn merge_headers(&self, event: &TriggeredEvent) -> HashMap<String, String> {
        let mut headers = self.headers.clone();

        // Merge headers from event metadata if present
        if let Some(ref metadata) = event.metadata
            && let Some(extra_headers) = metadata.get("headers").and_then(|v| v.as_object()) {
                for (key, value) in extra_headers {
                    if let Some(v) = value.as_str() {
                        headers.insert(key.clone(), v.to_string());
                    }
                }
            }

        headers
    }
}

/// Request data grouped by URL.
struct UrlBatch {
    headers: HashMap<String, String>,
    payloads: Vec<serde_json::Value>,
}

impl Sink for WebhookSink {
    fn name() -> &'static str {
        "webhook"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Group events by URL to send batched requests.
        let mut url_batches: HashMap<String, UrlBatch> = HashMap::new();

        for event in events {
            let url = self.resolve_url(&event).ok_or_else(|| {
                etl::etl_error!(
                    etl::error::ErrorKind::ConfigError,
                    "No URL configured",
                    "URL must be provided in sink config or event metadata"
                )
            })?;

            let batch = url_batches
                .entry(url.to_string())
                .or_insert_with(|| UrlBatch {
                    headers: self.merge_headers(&event),
                    payloads: Vec::new(),
                });

            batch.payloads.push(event.payload);
        }

        // Send one request per URL with array of payloads.
        try_join_all(url_batches.into_iter().map(|(url, batch)| {
            let client = &self.client;
            async move {
                let mut request = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .json(&batch.payloads);

                for (key, value) in &batch.headers {
                    request = request.header(key, value);
                }

                let response = request.send().await.map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::DestinationError,
                        "Failed to send webhook request",
                        e.to_string()
                    )
                })?;

                if !response.status().is_success() {
                    return Err(etl::etl_error!(
                        etl::error::ErrorKind::DestinationError,
                        "Webhook request failed",
                        format!("HTTP status: {}", response.status())
                    ));
                }

                Ok(())
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
        assert_eq!(WebhookSink::name(), "webhook");
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(DEFAULT_TIMEOUT_MS, 30000);
    }
}
