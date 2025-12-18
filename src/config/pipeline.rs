//! Configuration management for pgstream daemon.
//!
//! Loads configuration from YAML files and environment variables,
//! following the etl-config pattern.

use etl_config::Config;
use serde::Deserialize;

use crate::config::{SinkConfig, StreamConfig};

/// Configuration for the pipeline.
///
/// Defines the stream settings and sink destination for the long-running stream process.
#[derive(Clone, Debug, Deserialize)]
pub struct PipelineConfig {
    /// Stream/pipeline configuration including source database and batching.
    pub stream: StreamConfig,
    /// Destination sink configuration.
    pub sink: SinkConfig,
}

impl Config for PipelineConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &[];
}
