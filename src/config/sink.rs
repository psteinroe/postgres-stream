use serde::Deserialize;

#[cfg(feature = "sink-nats")]
use crate::sink::nats::NatsSinkConfig;

#[cfg(feature = "sink-redis-strings")]
use crate::sink::redis_strings::RedisStringsSinkConfig;

#[cfg(feature = "sink-redis-streams")]
use crate::sink::redis_streams::RedisStreamsSinkConfig;

/// Sink destination configuration.
///
/// Determines where replicated events are sent.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    /// In-memory sink for testing and development.
    Memory,

    /// Redis strings sink for key-value storage.
    #[cfg(feature = "sink-redis-strings")]
    #[serde(rename = "redis-strings")]
    RedisStrings(RedisStringsSinkConfig),

    /// Redis streams sink for append-only log storage.
    #[cfg(feature = "sink-redis-streams")]
    #[serde(rename = "redis-streams")]
    RedisStreams(RedisStreamsSinkConfig),

    /// NATS sink for pub/sub messaging.
    #[cfg(feature = "sink-nats")]
    #[serde(rename = "nats")]
    Nats(NatsSinkConfig),
}
