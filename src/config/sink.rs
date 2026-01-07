//! Sink configuration types.
//!
//! Defines configuration variants for different event destinations.

use serde::Deserialize;

#[cfg(feature = "sink-elasticsearch")]
use crate::sink::elasticsearch::ElasticsearchSinkConfig;

/// Sink destination configuration.
///
/// Determines where replicated events are sent.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    /// In-memory sink for testing and development.
    Memory,

    /// Elasticsearch sink for document indexing.
    #[cfg(feature = "sink-elasticsearch")]
    Elasticsearch(ElasticsearchSinkConfig),
}
