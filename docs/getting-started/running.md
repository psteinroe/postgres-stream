# Running Postgres Stream

How to run Postgres Stream in different environments.

## Docker (Recommended)

Each sink has its own Docker image for minimal size:

```bash
# Pull the image for your sink
docker pull ghcr.io/psteinroe/postgres-stream:kafka-latest

# Run with your config
docker run -v $(pwd)/config.yaml:/config.yaml \
  ghcr.io/psteinroe/postgres-stream:kafka-latest
```

### Available Images

| Sink | Image Tag |
|------|-----------|
| Kafka | `kafka-latest` |
| NATS | `nats-latest` |
| RabbitMQ | `rabbitmq-latest` |
| Redis Strings | `redis-strings-latest` |
| Redis Streams | `redis-streams-latest` |
| Webhook | `webhook-latest` |
| AWS SQS | `sqs-latest` |
| AWS SNS | `sns-latest` |
| AWS Kinesis | `kinesis-latest` |
| GCP Pub/Sub | `gcp-pubsub-latest` |
| Elasticsearch | `elasticsearch-latest` |
| Meilisearch | `meilisearch-latest` |

### Version Tags

For production, use specific version tags:

```bash
docker pull ghcr.io/psteinroe/postgres-stream:kafka-v1.0.0
```

## Docker Compose

Example `docker-compose.yml`:

```yaml
version: '3.8'

services:
  postgres-stream:
    image: ghcr.io/psteinroe/postgres-stream:kafka-latest
    volumes:
      - ./config.yaml:/config.yaml
    depends_on:
      - postgres
      - kafka
    restart: unless-stopped

  postgres:
    image: postgres:16
    command: postgres -c wal_level=logical
    environment:
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"

  kafka:
    image: confluentinc/cp-kafka:latest
    # ... kafka configuration
```

## Binary

Build from source:

```bash
# Clone the repository
git clone https://github.com/psteinroe/postgres-stream
cd postgres-stream

# Build with specific sink
cargo build --release --features sink-kafka

# Run
./target/release/postgres-stream
```

### Feature Flags

Build only the sinks you need:

```bash
# Single sink
cargo build --release --features sink-kafka

# Multiple sinks
cargo build --release --features "sink-kafka,sink-webhook"
```

## Health Checks

Postgres Stream exposes Prometheus metrics on port 9090 by default:

```bash
curl http://localhost:9090/metrics
```

## Graceful Shutdown

Postgres Stream handles `SIGTERM` and `SIGINT` gracefully:

1. Stops accepting new events
2. Completes processing of current batch
3. Checkpoints progress
4. Exits cleanly

In Kubernetes, configure `terminationGracePeriodSeconds` appropriately:

```yaml
spec:
  terminationGracePeriodSeconds: 30
```

## Logging

Control log level with `RUST_LOG`:

```bash
RUST_LOG=info docker run ...

# Debug logging
RUST_LOG=debug docker run ...

# Specific module logging
RUST_LOG=postgres_stream=debug docker run ...
```
