//! Sink configuration types.
//!
//! Defines configuration variants for different event destinations.

use serde::Deserialize;

#[cfg(feature = "sink-meilisearch")]
use crate::sink::meilisearch::MeilisearchSinkConfig;

/// Sink destination configuration.
///
/// Determines where replicated events are sent.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    /// In-memory sink for testing and development.
    Memory,

    /// Meilisearch sink for search engine indexing.
    #[cfg(feature = "sink-meilisearch")]
    Meilisearch(MeilisearchSinkConfig),
}
