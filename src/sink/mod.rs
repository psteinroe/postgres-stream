mod base;
pub mod memory;

#[cfg(feature = "sink-elasticsearch")]
pub mod elasticsearch;

#[cfg(feature = "sink-nats")]
pub mod nats;

#[cfg(any(feature = "sink-redis-strings", feature = "sink-redis-streams"))]
mod redis_common;

#[cfg(feature = "sink-redis-strings")]
pub mod redis_strings;

#[cfg(feature = "sink-redis-streams")]
pub mod redis_streams;

#[cfg(feature = "sink-rabbitmq")]
pub mod rabbitmq;

#[cfg(feature = "sink-webhook")]
pub mod webhook;

#[cfg(feature = "sink-kafka")]
pub mod kafka;

#[cfg(feature = "sink-sqs")]
pub mod sqs;

#[cfg(feature = "sink-sns")]
pub mod sns;

#[cfg(feature = "sink-kinesis")]
pub mod kinesis;

#[cfg(feature = "sink-meilisearch")]
pub mod meilisearch;

#[cfg(feature = "sink-gcp-pubsub")]
pub mod gcp_pubsub;

pub use base::Sink;

use etl::error::EtlResult;
use memory::MemorySink;

#[cfg(feature = "sink-elasticsearch")]
use elasticsearch::ElasticsearchSink;

#[cfg(feature = "sink-nats")]
use nats::NatsSink;

#[cfg(feature = "sink-redis-strings")]
use redis_strings::RedisStringsSink;

#[cfg(feature = "sink-redis-streams")]
use redis_streams::RedisStreamsSink;

#[cfg(feature = "sink-rabbitmq")]
use rabbitmq::RabbitmqSink;

#[cfg(feature = "sink-webhook")]
use webhook::WebhookSink;

#[cfg(feature = "sink-kafka")]
use kafka::KafkaSink;

#[cfg(feature = "sink-sqs")]
use sqs::SqsSink;

#[cfg(feature = "sink-sns")]
use sns::SnsSink;

#[cfg(feature = "sink-kinesis")]
use kinesis::KinesisSink;

#[cfg(feature = "sink-meilisearch")]
use meilisearch::MeilisearchSink;

#[cfg(feature = "sink-gcp-pubsub")]
use gcp_pubsub::GcpPubsubSink;

use crate::types::TriggeredEvent;

/// Wrapper enum for all supported sink types.
///
/// Enables runtime sink selection while maintaining static dispatch.
/// Each variant wraps a concrete sink implementation gated by its feature flag.
#[derive(Clone)]
pub enum AnySink {
    /// In-memory sink for testing and development.
    Memory(MemorySink),

    /// Elasticsearch sink for document indexing.
    #[cfg(feature = "sink-elasticsearch")]
    Elasticsearch(ElasticsearchSink),

    /// Redis strings sink for key-value storage.
    #[cfg(feature = "sink-redis-strings")]
    RedisStrings(RedisStringsSink),

    /// Redis streams sink for append-only log storage.
    #[cfg(feature = "sink-redis-streams")]
    RedisStreams(RedisStreamsSink),

    /// NATS sink for pub/sub messaging.
    #[cfg(feature = "sink-nats")]
    Nats(NatsSink),

    /// RabbitMQ sink for AMQP messaging.
    #[cfg(feature = "sink-rabbitmq")]
    Rabbitmq(RabbitmqSink),

    /// Webhook sink for HTTP POST delivery.
    #[cfg(feature = "sink-webhook")]
    Webhook(WebhookSink),

    /// Kafka sink for Apache Kafka messaging.
    #[cfg(feature = "sink-kafka")]
    Kafka(KafkaSink),

    /// AWS SQS sink for queue messaging.
    #[cfg(feature = "sink-sqs")]
    Sqs(SqsSink),

    /// AWS SNS sink for topic publishing.
    #[cfg(feature = "sink-sns")]
    Sns(SnsSink),

    /// AWS Kinesis sink for data stream publishing.
    #[cfg(feature = "sink-kinesis")]
    Kinesis(KinesisSink),

    /// Meilisearch sink for document indexing.
    #[cfg(feature = "sink-meilisearch")]
    Meilisearch(MeilisearchSink),

    /// GCP Pub/Sub sink for topic publishing.
    #[cfg(feature = "sink-gcp-pubsub")]
    GcpPubsub(GcpPubsubSink),
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

            #[cfg(feature = "sink-redis-strings")]
            AnySink::RedisStrings(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-redis-streams")]
            AnySink::RedisStreams(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-nats")]
            AnySink::Nats(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-rabbitmq")]
            AnySink::Rabbitmq(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-webhook")]
            AnySink::Webhook(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-kafka")]
            AnySink::Kafka(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-sqs")]
            AnySink::Sqs(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-sns")]
            AnySink::Sns(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-kinesis")]
            AnySink::Kinesis(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-meilisearch")]
            AnySink::Meilisearch(sink) => sink.publish_events(events).await,

            #[cfg(feature = "sink-gcp-pubsub")]
            AnySink::GcpPubsub(sink) => sink.publish_events(events).await,
        }
    }
}
