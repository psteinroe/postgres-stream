use serde::Deserialize;

#[cfg(feature = "sink-redis-strings")]
use crate::sink::redis_strings::RedisStringsSinkConfig;

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
}
