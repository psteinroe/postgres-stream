use serde::Deserialize;

#[cfg(feature = "sink-kafka")]
use crate::sink::kafka::KafkaSinkConfig;

#[cfg(feature = "sink-nats")]
use crate::sink::nats::NatsSinkConfig;

#[cfg(feature = "sink-redis-strings")]
use crate::sink::redis_strings::RedisStringsSinkConfig;

#[cfg(feature = "sink-redis-streams")]
use crate::sink::redis_streams::RedisStreamsSinkConfig;

#[cfg(feature = "sink-rabbitmq")]
use crate::sink::rabbitmq::RabbitmqSinkConfig;

#[cfg(feature = "sink-webhook")]
use crate::sink::webhook::WebhookSinkConfig;

#[cfg(feature = "sink-sqs")]
use crate::sink::sqs::SqsSinkConfig;

#[cfg(feature = "sink-sns")]
use crate::sink::sns::SnsSinkConfig;

#[cfg(feature = "sink-kinesis")]
use crate::sink::kinesis::KinesisSinkConfig;

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

    /// RabbitMQ sink for AMQP messaging.
    #[cfg(feature = "sink-rabbitmq")]
    #[serde(rename = "rabbitmq")]
    Rabbitmq(RabbitmqSinkConfig),

    /// Webhook sink for HTTP POST delivery.
    #[cfg(feature = "sink-webhook")]
    #[serde(rename = "webhook")]
    Webhook(WebhookSinkConfig),

    /// Kafka sink for Apache Kafka messaging.
    #[cfg(feature = "sink-kafka")]
    #[serde(rename = "kafka")]
    Kafka(KafkaSinkConfig),

    /// AWS SQS sink for queue messaging.
    #[cfg(feature = "sink-sqs")]
    #[serde(rename = "sqs")]
    Sqs(SqsSinkConfig),

    /// AWS SNS sink for topic publishing.
    #[cfg(feature = "sink-sns")]
    #[serde(rename = "sns")]
    Sns(SnsSinkConfig),

    /// AWS Kinesis sink for data stream publishing.
    #[cfg(feature = "sink-kinesis")]
    #[serde(rename = "kinesis")]
    Kinesis(KinesisSinkConfig),
}
