# Kafka

High-throughput event streaming to Apache Kafka.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:kafka-latest
```

## Configuration

```yaml
sink:
  type: kafka
  brokers: localhost:9092
  topic: events
```

### With Authentication

```yaml
sink:
  type: kafka
  brokers: broker.example.com:9092
  topic: events
  sasl_mechanism: PLAIN
  sasl_username: ${KAFKA_USERNAME}
  sasl_password: ${KAFKA_PASSWORD}
  security_protocol: SASL_SSL
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `brokers` | string | Yes | - | No | Comma-separated list of Kafka brokers |
| `topic` | string | No | - | Yes | Default topic (can be overridden per-event) |
| `sasl_username` | string | No | - | No | SASL username for authentication |
| `sasl_password` | string | No | - | No | SASL password for authentication |
| `sasl_mechanism` | string | No | - | No | SASL mechanism (PLAIN, SCRAM-SHA-256) |
| `security_protocol` | string | No | - | No | Security protocol (SASL_SSL, SASL_PLAINTEXT) |
| `delivery_timeout_ms` | integer | No | 5000 | No | Message delivery timeout |

## Dynamic Routing

Route events to different topics using metadata:

```sql
-- Route by table name
metadata_extensions = '[
  {"json_path": "topic", "expression": "''events-'' || tg_table_name"}
]'
```

The sink reads the `topic` key from event metadata.

## Example

Complete configuration:

```yaml
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
  topic: postgres-events
```

Events are produced with:
- **Key**: Event ID
- **Value**: JSON-serialized payload
