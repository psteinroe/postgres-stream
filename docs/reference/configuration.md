# Configuration Reference

Complete reference for `config.yaml` options.

## Full Example

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
      trusted_root_certs: ""
    keepalive:
      idle_secs: 30
      interval_secs: 10
      retries: 3
  batch:
    memory_budget_ratio: 0.2
    max_fill_ms: 5000

sink:
  type: kafka
  brokers: localhost:9092
  topic: events
```

## Stream Configuration

### `stream.id`

| | |
|--|--|
| Type | integer |
| Required | Yes |

Unique identifier for this stream. Must match the `stream_id` in subscription records.

### `stream.pg_connection`

Database connection settings.

#### `host`

| | |
|--|--|
| Type | string |
| Required | Yes |

Database hostname or IP address.

#### `port`

| | |
|--|--|
| Type | integer |
| Required | Yes |
| Default | 5432 |

Database port.

#### `name`

| | |
|--|--|
| Type | string |
| Required | Yes |

Database name.

#### `username`

| | |
|--|--|
| Type | string |
| Required | Yes |

Database username. Must have REPLICATION privilege.

#### `password`

| | |
|--|--|
| Type | string |
| Required | Yes |

Database password. Supports environment variable substitution: `${ENV_VAR}`.

#### `tls.enabled`

| | |
|--|--|
| Type | boolean |
| Required | No |
| Default | false |

Enable TLS for database connection.

#### `tls.trusted_root_certs`

| | |
|--|--|
| Type | string |
| Required | No |

Path to CA certificate file for TLS verification.

#### `keepalive`

TCP keepalive settings. All fields are optional and have sensible defaults.

##### `keepalive.idle_secs`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 30 |

Seconds of idle time before the first keepalive probe is sent.

##### `keepalive.interval_secs`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 10 |

Seconds between successive keepalive probes.

##### `keepalive.retries`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 3 |

Number of failed probes before the connection is considered dead.

### `stream.batch`

Controls how events are grouped before delivery.

#### `memory_budget_ratio`

| | |
|--|--|
| Type | float |
| Required | No |
| Default | 0.2 |

Fraction of total process memory reserved for batched stream payloads. Higher values allow larger batches but increase memory usage.

#### `max_fill_ms`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 10000 |

Maximum time to fill a batch in milliseconds. Lower values reduce latency but may result in smaller batches.

## Sink Configuration

The `sink` section defines the delivery destination. Each sink type has different options.

### Common Pattern

```yaml
sink:
  type: <sink-type>
  # sink-specific options
```

### Available Types

- `memory` - Built-in test sink
- `kafka` - Apache Kafka
- `nats` - NATS messaging
- `rabbitmq` - RabbitMQ
- `redis-strings` - Redis key-value
- `redis-streams` - Redis Streams
- `webhook` - HTTP POST
- `sqs` - AWS SQS
- `sns` - AWS SNS
- `kinesis` - AWS Kinesis
- `gcp-pubsub` - GCP Pub/Sub
- `elasticsearch` - Elasticsearch
- `meilisearch` - Meilisearch

See the [Sinks](../sinks/index.md) section for sink-specific configuration.

## Environment Variables

Use `${VAR_NAME}` syntax to substitute environment variables:

```yaml
stream:
  pg_connection:
    password: ${POSTGRES_PASSWORD}

sink:
  type: kafka
  sasl_password: ${KAFKA_PASSWORD}
```

## Configuration File Location

By default, Postgres Stream looks for `config.yaml` in the current directory.

Override with the `CONFIG_PATH` environment variable:

```bash
CONFIG_PATH=/etc/postgres-stream/config.yaml postgres-stream
```
