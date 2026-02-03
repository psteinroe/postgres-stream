<p align="center">
  <img src="media/logo.png" alt="Postgres Stream" width="300">
</p>

<h1 align="center">Postgres Stream</h1>

<p align="center">
  Reliably stream Postgres table changes to external systems with automatic failover and zero event loss.
</p>

## What is Postgres Stream?

Postgres Stream captures changes from your Postgres tables and delivers them to external systems like Kafka, RabbitMQ, Redis, Webhooks, and cloud services. It uses Postgres native logical replication and stores events durably in the database itself.

## Key Features

- **Single binary** - No complex infrastructure or high-availability destinations required
- **Postgres-native durability** - Events are stored in the database, WAL can be released immediately
- **Zero data loss** - As long as downtime is less than partition retention (7 days by default)
- **Automatic recovery** - Handles both sink failures and slot invalidation without operator intervention

## How It Works

Events are inserted into the `pgstream.events` table and streamed via logical replication to your sink.

**Two ways to create events:**

1. **Subscriptions** (optional) - Define triggers that automatically capture table changes
2. **Manual inserts** - Insert directly into `pgstream.events` from your application or database functions

## Trade-offs

While Postgres Stream provides strong durability guarantees, there are some considerations:

- **Small overhead** - Additional INSERT into `events` table on every subscribed operation
- **Partition management** - Monitor partition growth if event volume is very high
- **Not for dynamic subscriptions** - Each subscription change recreates database triggers

## Supported Sinks

| Sink | Use Case |
|------|----------|
| [Kafka](sinks/kafka.md) | High-throughput event streaming |
| [NATS](sinks/nats.md) | Lightweight pub/sub messaging |
| [RabbitMQ](sinks/rabbitmq.md) | Enterprise message broker |
| [Redis Strings](sinks/redis-strings.md) | Key-value caching |
| [Redis Streams](sinks/redis-streams.md) | Append-only event log |
| [Webhook](sinks/webhook.md) | HTTP POST delivery |
| [AWS SQS](sinks/sqs.md) | Managed queue service |
| [AWS SNS](sinks/sns.md) | Managed pub/sub service |
| [AWS Kinesis](sinks/kinesis.md) | Real-time data streaming |
| [GCP Pub/Sub](sinks/gcp-pubsub.md) | Google Cloud messaging |
| [Elasticsearch](sinks/elasticsearch.md) | Search indexing |
| [Meilisearch](sinks/meilisearch.md) | Search indexing |

## Quick Start

```yaml
# config.yaml
stream:
  id: 1
  pg_connection:
    host: localhost
    port: 5432
    name: mydb
    username: postgres
    password: postgres
    tls:
      enabled: false
  batch:
    max_size: 1000
    max_fill_secs: 5

sink:
  type: kafka
  brokers: localhost:9092
  topic: events
```

```bash
# Run with Docker
docker run -v $(pwd)/config.yaml:/config.yaml \
  ghcr.io/psteinroe/postgres-stream:kafka-latest
```

## Next Steps

- [Getting Started](getting-started.md) - Set up your first stream
- [How It Works](concepts/how-it-works.md) - Understand the architecture
- [Sinks](sinks/index.md) - Choose your destination
