<p align="center">
  <img src="docs/media/logo.png" alt="Postgres Stream" width="300">
</p>

<h1 align="center">Postgres Stream</h1>

<p align="center">
  Reliably stream Postgres table changes to external systems with automatic failover and zero event loss.
</p>

## Features

- **Single binary** - No complex infrastructure or high-availability destinations required
- **Postgres-native durability** - Events are stored in the database, WAL can be released immediately
- **Zero data loss** - As long as downtime is less than partition retention (7 days by default)
- **Automatic recovery** - Handles both sink failures and slot invalidation without operator intervention

## Supported Sinks

Kafka, NATS, RabbitMQ, Redis Strings, Redis Streams, Webhook, AWS SQS, AWS SNS, AWS Kinesis, GCP Pub/Sub, Elasticsearch, Meilisearch

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
  type: webhook
  url: https://httpbin.org/post
```

```bash
# Run with Docker
docker run -v $(pwd)/config.yaml:/config.yaml \
  ghcr.io/psteinroe/postgres-stream:webhook-latest

# Create a subscription
psql -c "
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name)
VALUES ('user-created', 1, 'INSERT', 'public', 'users');
"
```

## Documentation

Full documentation available at **[psteinroe.github.io/postgres-stream](https://psteinroe.github.io/postgres-stream/)**

- [Getting Started](https://psteinroe.github.io/postgres-stream/getting-started/)
- [How It Works](https://psteinroe.github.io/postgres-stream/concepts/how-it-works/)
- [Sinks](https://psteinroe.github.io/postgres-stream/sinks/)
- [Configuration Reference](https://psteinroe.github.io/postgres-stream/reference/configuration/)

## License

MIT
