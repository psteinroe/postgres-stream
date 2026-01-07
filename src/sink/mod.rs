//! Sink implementations for event publishing.
//!
//! Provides destinations for replicated PostgreSQL events.

mod base;
pub mod memory;

#[cfg(feature = "sink-elasticsearch")]
pub mod elasticsearch;

pub use base::Sink;

use etl::error::EtlResult;
use memory::MemorySink;

#[cfg(feature = "sink-elasticsearch")]
use elasticsearch::ElasticsearchSink;

use crate::types::TriggeredEvent;

/// Wrapper enum for all supported sink types.
///
/// Enables runtime sink selection while maintaining static dispatch
/// for better performance. Each variant wraps a concrete sink type.
#[derive(Clone)]
pub enum AnySink {
    /// In-memory sink for testing and development.
    Memory(MemorySink),

    #[cfg(feature = "sink-elasticsearch")]
    /// Elasticsearch sink for document indexing.
    Elasticsearch(ElasticsearchSink),
}

impl Sink for AnySink {
    fn name() -> &'static str {
        "any"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        match self {
            AnySink::Memory(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-elasticsearch")]
            AnySink::Elasticsearch(sink) => sink.publish_events(events).await,
        }
    }
}
