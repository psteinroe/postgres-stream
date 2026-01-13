mod base;
pub mod memory;

#[cfg(feature = "sink-nats")]
pub mod nats;

#[cfg(feature = "sink-redis-strings")]
pub mod redis_strings;

#[cfg(feature = "sink-redis-streams")]
pub mod redis_streams;

pub use base::Sink;

use etl::error::EtlResult;
use memory::MemorySink;

#[cfg(feature = "sink-nats")]
use nats::NatsSink;

#[cfg(feature = "sink-redis-strings")]
use redis_strings::RedisStringsSink;

#[cfg(feature = "sink-redis-streams")]
use redis_streams::RedisStreamsSink;

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

    /// Redis streams sink for append-only log storage.
    #[cfg(feature = "sink-redis-streams")]
    RedisStreams(RedisStreamsSink),

    /// NATS sink for pub/sub messaging.
    #[cfg(feature = "sink-nats")]
    Nats(NatsSink),
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

            #[cfg(feature = "sink-redis-streams")]
            AnySink::RedisStreams(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-nats")]
            AnySink::Nats(sink) => sink.publish_events(events).await,
        }
    }
}
