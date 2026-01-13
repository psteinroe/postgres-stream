# Sinks

Choose where to deliver your events.

## Overview

Sinks define where Postgres Stream delivers events. Each sink type has a dedicated Docker image for minimal size.

## Available Sinks

| Sink | Docker Image | Use Case |
|------|--------------|----------|
| [Kafka](kafka.md) | `kafka-latest` | High-throughput event streaming |
| [NATS](nats.md) | `nats-latest` | Lightweight pub/sub messaging |
| [RabbitMQ](rabbitmq.md) | `rabbitmq-latest` | Enterprise message broker |
| [Redis Strings](redis-strings.md) | `redis-strings-latest` | Key-value caching |
| [Redis Streams](redis-streams.md) | `redis-streams-latest` | Append-only event log |
| [Webhook](webhook.md) | `webhook-latest` | HTTP POST delivery |
| [AWS SQS](sqs.md) | `sqs-latest` | Managed queue service |
| [AWS SNS](sns.md) | `sns-latest` | Managed pub/sub service |
| [AWS Kinesis](kinesis.md) | `kinesis-latest` | Real-time data streaming |
| [GCP Pub/Sub](gcp-pubsub.md) | `gcp-pubsub-latest` | Google Cloud messaging |
| [Elasticsearch](elasticsearch.md) | `elasticsearch-latest` | Search indexing |
| [Meilisearch](meilisearch.md) | `meilisearch-latest` | Search indexing |

## Dynamic Routing

Most sinks support dynamic routing via event metadata. This lets you route events to different destinations based on row data:

```sql
-- Route to different topics based on table name
insert into pgstream.subscriptions (
  key, stream_id, operation, schema_name, table_name,
  column_names, metadata_extensions
) values (
  'all-events',
  1,
  'INSERT',
  'public',
  'orders',
  array['id', 'user_id'],
  '[{"json_path": "topic", "expression": "''events-'' || tg_table_name"}]'::jsonb
);
```

See each sink's documentation for supported metadata fields.

## Choosing a Sink

**For high throughput**: Kafka, Kinesis, or GCP Pub/Sub

**For simple integration**: Webhook

**For AWS infrastructure**: SQS, SNS, or Kinesis

**For GCP infrastructure**: GCP Pub/Sub

**For real-time messaging**: NATS or RabbitMQ

**For caching**: Redis Strings

**For event log**: Redis Streams

**For search indexing**: Elasticsearch or Meilisearch
