//! AWS SNS sink for publishing events to an Amazon SNS topic.
//!
//! Publishes each event's payload as JSON to a topic ARN determined by:
//! 1. `topic` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `topic_arn` in sink config
//!
//! # Dynamic Routing
//!
//! The target topic ARN can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "topic", "expression": "''arn:aws:sns:us-east-1:123456789:'' || topic_name"}
//! ]'
//! ```

use aws_sdk_sns::Client;
use aws_sdk_sns::types::PublishBatchRequestEntry;
use etl::error::EtlResult;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Maximum messages per SNS batch (AWS limit).
const SNS_MAX_BATCH_SIZE: usize = 10;

/// Configuration for the AWS SNS sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (AWS credentials, endpoint URLs) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct SnsSinkConfig {
    /// SNS topic ARN to publish messages to. Optional if provided via event metadata.
    pub topic_arn: Option<String>,

    /// AWS region (e.g., "us-east-1").
    pub region: String,

    /// Optional custom endpoint URL for LocalStack testing.
    #[serde(default)]
    pub endpoint_url: Option<String>,

    /// Optional AWS access key ID (uses default credentials chain if not set).
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// Optional AWS secret access key (uses default credentials chain if not set).
    #[serde(default)]
    pub secret_access_key: Option<String>,
}

/// Configuration for the AWS SNS sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnsSinkConfigWithoutSecrets {
    /// SNS topic ARN to publish messages to (if configured).
    pub topic_arn: Option<String>,

    /// AWS region.
    pub region: String,

    /// Whether a custom endpoint is configured.
    pub has_custom_endpoint: bool,

    /// Whether explicit credentials are configured.
    pub has_explicit_credentials: bool,
}

impl From<SnsSinkConfig> for SnsSinkConfigWithoutSecrets {
    fn from(config: SnsSinkConfig) -> Self {
        Self {
            topic_arn: config.topic_arn,
            region: config.region,
            has_custom_endpoint: config.endpoint_url.is_some(),
            has_explicit_credentials: config.access_key_id.is_some(),
        }
    }
}

impl From<&SnsSinkConfig> for SnsSinkConfigWithoutSecrets {
    fn from(config: &SnsSinkConfig) -> Self {
        Self {
            topic_arn: config.topic_arn.clone(),
            region: config.region.clone(),
            has_custom_endpoint: config.endpoint_url.is_some(),
            has_explicit_credentials: config.access_key_id.is_some(),
        }
    }
}

/// Sink that publishes events to an AWS SNS topic.
///
/// Each event's payload is serialized as JSON and published to the configured topic.
/// The sink uses the AWS SDK with automatic retry handling.
#[derive(Clone)]
pub struct SnsSink {
    /// AWS SNS client.
    client: Arc<Client>,

    /// Default topic ARN. Can be overridden per-event via metadata.
    topic_arn: Option<String>,
}

impl SnsSink {
    /// Creates a new SNS sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the AWS client cannot be configured.
    pub async fn new(
        config: SnsSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let region = aws_sdk_sns::config::Region::new(config.region);

        let mut sdk_config_loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

        // Configure custom endpoint if provided (for LocalStack).
        if let Some(endpoint) = &config.endpoint_url {
            sdk_config_loader = sdk_config_loader.endpoint_url(endpoint);
        }

        // Configure explicit credentials if provided.
        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            sdk_config_loader =
                sdk_config_loader.credentials_provider(aws_sdk_sns::config::Credentials::new(
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
            topic_arn: config.topic_arn,
        })
    }

    /// Resolves the topic ARN for an event from metadata or config.
    fn resolve_topic_arn<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic topic
        if let Some(ref metadata) = event.metadata
            && let Some(topic) = metadata.get("topic").and_then(|v| v.as_str()) {
                return Some(topic);
            }
        // Fall back to config topic ARN
        self.topic_arn.as_deref()
    }
}

impl Sink for SnsSink {
    fn name() -> &'static str {
        "sns"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Build entries and group by topic ARN.
        let mut entries_by_topic: HashMap<String, Vec<(usize, String)>> = HashMap::new();

        for (idx, event) in events.into_iter().enumerate() {
            let topic_arn = self.resolve_topic_arn(&event).ok_or_else(|| {
                etl::etl_error!(
                    etl::error::ErrorKind::ConfigError,
                    "No topic ARN configured",
                    "Topic ARN must be provided in sink config or event metadata"
                )
            })?;

            // SNS message is a string containing JSON.
            let message = event.payload.to_string();

            entries_by_topic
                .entry(topic_arn.to_string())
                .or_default()
                .push((idx, message));
        }

        // Create batch futures for all topics and chunks.
        let mut batch_futures = Vec::new();

        for (topic_arn, mut messages) in entries_by_topic {
            // Process in chunks of SNS_MAX_BATCH_SIZE.
            while !messages.is_empty() {
                let chunk_size = messages.len().min(SNS_MAX_BATCH_SIZE);
                let chunk: Vec<_> = messages.drain(..chunk_size).collect();

                let entries: Vec<_> = chunk
                    .into_iter()
                    .map(|(idx, message)| {
                        PublishBatchRequestEntry::builder()
                            .id(idx.to_string())
                            .message(message)
                            .build()
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        etl::etl_error!(
                            etl::error::ErrorKind::InvalidData,
                            "Failed to build batch entry",
                            e.to_string()
                        )
                    })?;

                let client = self.client.clone();
                let topic_arn = topic_arn.clone();
                batch_futures.push(async move {
                    let result = client
                        .publish_batch()
                        .topic_arn(&topic_arn)
                        .set_publish_batch_request_entries(Some(entries))
                        .send()
                        .await
                        .map_err(|e| {
                            etl::etl_error!(
                                etl::error::ErrorKind::DestinationError,
                                "Failed to publish batch to SNS",
                                e.to_string()
                            )
                        })?;

                    // Check for partial failures.
                    if !result.failed().is_empty() {
                        let failed_ids: Vec<_> = result.failed().iter().map(|f| f.id()).collect();
                        return Err(etl::etl_error!(
                            etl::error::ErrorKind::DestinationError,
                            "Some messages failed to publish to SNS",
                            format!("Failed message IDs: {:?}", failed_ids)
                        ));
                    }

                    Ok(())
                });
            }
        }

        try_join_all(batch_futures).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_name() {
        assert_eq!(SnsSink::name(), "sns");
    }

    #[test]
    fn test_config_without_secrets() {
        let config = SnsSinkConfig {
            topic_arn: Some("arn:aws:sns:us-east-1:123456789:my-topic".to_string()),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:4566".to_string()),
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
        };

        let without_secrets: SnsSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(
            without_secrets.topic_arn,
            Some("arn:aws:sns:us-east-1:123456789:my-topic".to_string())
        );
        assert_eq!(without_secrets.region, "us-east-1");
        assert!(without_secrets.has_custom_endpoint);
        assert!(without_secrets.has_explicit_credentials);
    }
}
