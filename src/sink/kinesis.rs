//! AWS Kinesis sink for publishing events to a Kinesis data stream.
//!
//! Publishes each event's payload as a data record to a stream determined by:
//! 1. `stream` key in event metadata (from subscription's metadata/metadata_extensions)
//! 2. Fallback to `stream_name` in sink config
//!
//! # Dynamic Routing
//!
//! The target stream can be configured per-event using metadata_extensions:
//!
//! ```sql
//! metadata_extensions = '[
//!   {"json_path": "stream", "expression": "''my-stream-'' || shard_key"}
//! ]'
//! ```

use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use etl::error::EtlResult;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// Maximum number of records per Kinesis PutRecords request.
const KINESIS_MAX_BATCH_SIZE: usize = 500;

/// Configuration for the AWS Kinesis sink.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets (AWS credentials, endpoint URLs) in serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct KinesisSinkConfig {
    /// Kinesis stream name. Optional if provided via event metadata.
    pub stream_name: Option<String>,

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

/// Configuration for the AWS Kinesis sink without sensitive data.
///
/// Safe to serialize and log. Use this for debugging and metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KinesisSinkConfigWithoutSecrets {
    /// Kinesis stream name (if configured).
    pub stream_name: Option<String>,

    /// AWS region.
    pub region: String,

    /// Whether a custom endpoint is configured.
    pub has_custom_endpoint: bool,

    /// Whether explicit credentials are configured.
    pub has_explicit_credentials: bool,
}

impl From<KinesisSinkConfig> for KinesisSinkConfigWithoutSecrets {
    fn from(config: KinesisSinkConfig) -> Self {
        Self {
            stream_name: config.stream_name,
            region: config.region,
            has_custom_endpoint: config.endpoint_url.is_some(),
            has_explicit_credentials: config.access_key_id.is_some(),
        }
    }
}

impl From<&KinesisSinkConfig> for KinesisSinkConfigWithoutSecrets {
    fn from(config: &KinesisSinkConfig) -> Self {
        Self {
            stream_name: config.stream_name.clone(),
            region: config.region.clone(),
            has_custom_endpoint: config.endpoint_url.is_some(),
            has_explicit_credentials: config.access_key_id.is_some(),
        }
    }
}

/// Sink that publishes events to an AWS Kinesis data stream.
///
/// Each event's payload is serialized as JSON and published as a data record.
/// The sink uses the AWS SDK with automatic retry handling.
#[derive(Clone)]
pub struct KinesisSink {
    /// AWS Kinesis client.
    client: Arc<Client>,

    /// Default stream name. Can be overridden per-event via metadata.
    stream_name: Option<String>,
}

impl KinesisSink {
    /// Creates a new Kinesis sink from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the AWS client cannot be configured.
    pub async fn new(
        config: KinesisSinkConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let region = aws_sdk_kinesis::config::Region::new(config.region);

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
                sdk_config_loader.credentials_provider(aws_sdk_kinesis::config::Credentials::new(
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
            stream_name: config.stream_name,
        })
    }

    /// Resolves the stream name for an event from metadata or config.
    fn resolve_stream_name<'a>(&'a self, event: &'a TriggeredEvent) -> Option<&'a str> {
        // First check event metadata for dynamic stream
        if let Some(ref metadata) = event.metadata {
            if let Some(stream) = metadata.get("stream").and_then(|v| v.as_str()) {
                return Some(stream);
            }
        }
        // Fall back to config stream name
        self.stream_name.as_deref()
    }
}

impl Sink for KinesisSink {
    fn name() -> &'static str {
        "kinesis"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Build records and group by stream in a single pass.
        let mut records_by_stream: HashMap<String, Vec<PutRecordsRequestEntry>> = HashMap::new();

        for event in events {
            // Convert to owned String to end borrow before moving event fields.
            let stream_name = self
                .resolve_stream_name(&event)
                .ok_or_else(|| {
                    etl::etl_error!(
                        etl::error::ErrorKind::ConfigError,
                        "No stream name configured",
                        "Stream name must be provided in sink config or event metadata"
                    )
                })?
                .to_string();

            let data = serde_json::to_vec(&event.payload).map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to serialize payload to JSON",
                    e.to_string()
                )
            })?;

            let record = PutRecordsRequestEntry::builder()
                .partition_key(event.id.id)
                .data(aws_sdk_kinesis::primitives::Blob::new(data))
                .build()
                .map_err(|e| {
                    etl::etl_error!(
                        etl::error::ErrorKind::InvalidData,
                        "Failed to build record entry",
                        e.to_string()
                    )
                })?;

            records_by_stream
                .entry(stream_name)
                .or_default()
                .push(record);
        }

        // Create batch futures for all streams and chunks.
        let mut batch_futures = Vec::new();

        for (stream_name, mut records) in records_by_stream {
            // Process in chunks of KINESIS_MAX_BATCH_SIZE.
            while !records.is_empty() {
                let chunk_size = records.len().min(KINESIS_MAX_BATCH_SIZE);
                let chunk: Vec<_> = records.drain(..chunk_size).collect();
                let chunk_len = chunk.len();

                let client = self.client.clone();
                let stream_name = stream_name.clone();
                batch_futures.push(async move {
                    let result = client
                        .put_records()
                        .stream_name(&stream_name)
                        .set_records(Some(chunk))
                        .send()
                        .await
                        .map_err(|e| {
                            etl::etl_error!(
                                etl::error::ErrorKind::DestinationError,
                                "Failed to publish records to Kinesis",
                                e.to_string()
                            )
                        })?;

                    // Check for partial failures.
                    let failed_count = result.failed_record_count.unwrap_or(0);
                    if failed_count > 0 {
                        let first_error = result
                            .records
                            .iter()
                            .find(|r| r.error_code.is_some())
                            .map(|r| {
                                format!(
                                    "{}: {}",
                                    r.error_code.as_deref().unwrap_or("Unknown"),
                                    r.error_message.as_deref().unwrap_or("No message")
                                )
                            })
                            .unwrap_or_else(|| "Unknown error".to_string());

                        return Err(etl::etl_error!(
                            etl::error::ErrorKind::DestinationError,
                            "Some records failed to publish to Kinesis",
                            format!(
                                "{} of {} records failed. First error: {}",
                                failed_count, chunk_len, first_error
                            )
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
        assert_eq!(KinesisSink::name(), "kinesis");
    }

    #[test]
    fn test_config_without_secrets() {
        let config = KinesisSinkConfig {
            stream_name: Some("my-stream".to_string()),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:4566".to_string()),
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some("secret".to_string()),
        };

        let without_secrets: KinesisSinkConfigWithoutSecrets = (&config).into();

        assert_eq!(without_secrets.stream_name, Some("my-stream".to_string()));
        assert_eq!(without_secrets.region, "us-east-1");
        assert!(without_secrets.has_custom_endpoint);
        assert!(without_secrets.has_explicit_credentials);
    }
}
