//! AWS SQS sink for publishing events to an Amazon SQS queue.
//!
//! Sends each event's payload as JSON to a queue URL determined by:
//! 1. `queue_url` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `queue_url` in sink config
//!
//! # Dynamic Routing
//!
//! The target queue URL can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "queue_url", "expression": "''https://sqs.us-east-1.amazonaws.com/123456789/'' || queue_name"}
//! ]'
//! ```

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use etl::error::EtlResult;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Maximum number of messages per SQS batch request.
const SQS_MAX_BATCH_SIZE: usize = 10;

/// Configuration for the AWS SQS sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (AWS credentials, endpoint URLs) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct SqsSinkConfig {
    /// SQS queue URL to send messages to. Optional if provided via event metadata.
    pub queue_url: Option<String>,

    /// AWS region (e.g., "us-east-1").
    pub region: String,

    /// Optional custom endpoint URL for LocalStack or ElasticMQ testing.
    #[serde(default)]
    pub endpoint_url: Option<String>,

    /// Optional AWS access key ID (uses default credentials chain if not set).
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// Optional AWS secret access key (uses default credentials chain if not set).
    #[serde(default)]
    pub secret_access_key: Option<String>,
}

/// Configuration for the AWS SQS sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqsSinkConfigWithoutSecrets {
    /// SQS queue URL to send messages to (if configured).
    pub queue_url: Option<String>,

    /// AWS region.
    pub region: String,

    /// Whether a custom endpoint is configured.
    pub has_custom_endpoint: bool,

    /// Whether explicit credentials are configured.
    pub has_explicit_credentials: bool,
}

impl From<SqsSinkConfig> for SqsSinkConfigWithoutSecrets {
    fn from(config: SqsSinkConfig) -> Self {
        Self {
            queue_url: config.queue_url,
            region: config.region,
            has_custom_endpoint: config.endpoint_url.is_some(),
            has_explicit_credentials: config.access_key_id.is_some(),
        }
    }
}

impl From<&SqsSinkConfig> for SqsSinkConfigWithoutSecrets {
    fn from(config: &SqsSinkConfig) -> Self {
        Self {
            queue_url: config.queue_url.clone(),
            region: config.region.clone(),
            has_custom_endpoint: config.endpoint_url.is_some(),
            has_explicit_credentials: config.access_key_id.is_some(),
        }
    }
}

/// Sink that sends events to an AWS SQS queue.
///
/// Each event's payload is sent as JSON to the configured queue.
/// The sink uses the AWS SDK with automatic retry handling.
#[derive(Clone)]
pub struct SqsSink {
    /// AWS SQS client.
    client: Arc<Client>,

    /// Default queue URL. Can be overridden per-event via metadata.
    queue_url: Option<String>,
}

impl SqsSink {
    /// Creates a new SQS sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the AWS client cannot be configured.
    pub async fn new(
        config: SqsSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let region = aws_sdk_sqs::config::Region::new(config.region);

        let mut sdk_config_loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

        // Configure custom endpoint if provided (for LocalStack/ElasticMQ).
        if let Some(endpoint) = &config.endpoint_url {
            sdk_config_loader = sdk_config_loader.endpoint_url(endpoint);
        }

        // Configure explicit credentials if provided.
        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            sdk_config_loader =
                sdk_config_loader.credentials_provider(aws_sdk_sqs::config::Credentials::new(
                    access_key.clone(),
                    secret_key.clone(),
                    None,
                    None,
                    "pgstream",
                ));
        }

        let sdk_config = sdk_config_loader.load().await;
        let client = Client::new(&sdk_config);

        Ok(Self {
            client: Arc::new(client),
            queue_url: config.queue_url,
        })
    }

    /// Resolves the queue URL for an event from metadata or config.
    fn resolve_queue_url<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic queue URL
        if let Some(ref metadata) = event.metadata {
            if let Some(queue_url) = metadata.get("queue_url").and_then(|v| v.as_str()) {
                return Some(queue_url);
            }
        }
        // Fall back to config queue URL
        self.queue_url.as_deref()
    }
}

impl Sink for SqsSink {
    fn name() -> &'static str {
        "sqs"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Group events by queue URL for batch sending.
        let mut events_by_queue: HashMap<String, Vec<(usize, TriggeredEvent)>> = HashMap::new();

        for (idx, event) in events.into_iter().enumerate() {
            let queue_url = self.resolve_queue_url(&event).ok_or_else(|| {
                etl::etl_error!(
                    etl::error::ErrorKind::ConfigError,
                    "No queue URL configured",
                    "Queue URL must be provided in sink config or event metadata"
                )
            })?;

            events_by_queue
                .entry(queue_url.to_string())
                .or_default()
                .push((idx, event));
        }

        // Prepare all batch requests.
        let mut batch_futures = Vec::new();

        for (queue_url, mut queue_events) in events_by_queue {
            // Process in chunks of SQS_MAX_BATCH_SIZE (SQS limit is 10 per batch).
            while !queue_events.is_empty() {
                let chunk_size = queue_events.len().min(SQS_MAX_BATCH_SIZE);
                let chunk: Vec<_> = queue_events.drain(..chunk_size).collect();

                let mut entries = Vec::with_capacity(chunk.len());

                for (idx, event) in chunk {
                    let entry = SendMessageBatchRequestEntry::builder()
                        .id(idx.to_string())
                        .message_body(event.payload.to_string())
                        .build()
                        .map_err(|e| {
                            etl::etl_error!(
                                etl::error::ErrorKind::InvalidData,
                                "Failed to build batch entry",
                                e.to_string()
                            )
                        })?;

                    entries.push(entry);
                }

                // Create future for this batch.
                let client = self.client.clone();
                let queue_url = queue_url.clone();
                batch_futures.push(async move {
                    let result = client
                        .send_message_batch()
                        .queue_url(&queue_url)
                        .set_entries(Some(entries))
                        .send()
                        .await
                        .map_err(|e| {
                            etl::etl_error!(
                                etl::error::ErrorKind::DestinationError,
                                "Failed to send batch to SQS",
                                e.to_string()
                            )
                        })?;

                    // Check for partial failures.
                    if !result.failed.is_empty() {
                        let failed_ids: Vec<_> = result.failed.iter().map(|f| f.id()).collect();
                        return Err(etl::etl_error!(
                            etl::error::ErrorKind::DestinationError,
                            "Some messages failed to send to SQS",
                            format!("Failed message IDs: {:?}", failed_ids)
                        ));
                    }

                    Ok(())
                });
            }
        }

        // Send all batches concurrently.
        try_join_all(batch_futures).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_name() {
        assert_eq!(SqsSink::name(), "sqs");
    }

    #[test]
    fn test_config_without_secrets() {
        let config = SqsSinkConfig {
            queue_url: Some("https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string()),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9324".to_string()),
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
        };

        let without_secrets: SqsSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(
            without_secrets.queue_url,
            Some("https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string())
        );
        assert_eq!(without_secrets.region, "us-east-1");
        assert!(without_secrets.has_custom_endpoint);
        assert!(without_secrets.has_explicit_credentials);
    }
}
