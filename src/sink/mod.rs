mod base;
pub mod memory;

#[cfg(feature = "sink-redis-strings")]
pub mod redis_strings;

pub use base::Sink;

use etl::error::EtlResult;
use memory::MemorySink;

#[cfg(feature = "sink-redis-strings")]
use redis_strings::RedisStringsSink;

use crate::types::TriggeredEvent;

/// Wrapper enum for all supported sink types.
///
/// Enables runtime sink selection while maintaining static dispatch.
/// Each variant wraps a concrete sink implementation gated by its feature flag.
#[derive(Clone)]
pub enum AnySink {
    /// In-memory sink for testing and development.
    Memory(MemorySink),

    /// Redis strings sink for key-value storage.
    #[cfg(feature = "sink-redis-strings")]
    RedisStrings(RedisStringsSink),
}

impl Sink for AnySink {
    fn name() -> &'static str {
        "any"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        match self {
            AnySink::Memory(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-redis-strings")]
            AnySink::RedisStrings(sink) => sink.publish_events(events).await,
        }
    }
}
