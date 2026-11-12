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
    max_size: 1000
    max_fill_secs: 5
  failover: false

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

Enable TLS for all database connections: replication, state, and failover replay. The server certificate and host name are verified against [`tls.trusted_root_certs`](#tlstrusted_root_certs). Replication and failover replay connections use SCRAM channel binding when the server offers it.

#### `tls.trusted_root_certs`

| | |
|--|--|
| Type | string |
| Required | When `tls.enabled` is true |

PEM-encoded CA certificates that sign the server certificate. This is the certificate content, not a path to a file. Startup fails with a configuration error if it is blank while `tls.enabled` is true.

```yaml
stream:
  pg_connection:
    tls:
      enabled: true
      trusted_root_certs: |
        -----BEGIN CERTIFICATE-----
        ...
        -----END CERTIFICATE-----
```

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

#### `max_size`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 1000 |

Maximum events per batch. Larger batches improve throughput but increase latency.

#### `max_fill_secs`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 5 |

Maximum time to fill a batch in seconds. Lower values reduce latency but may result in smaller batches.

### `stream.failover`

| | |
|--|--|
| Type | boolean |
| Required | No |
| Default | false |

Make the replication slot a [failover slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION) so it survives a switchover or failover to a standby. Requires Postgres 17 or newer; with an older server the stream fails to start when this is enabled.

When enabled, a new slot is created with `FAILOVER`, and an existing slot is altered to be a failover slot on startup, without resyncing. A slot recreated after [slot invalidation](../concepts/failover.md#slot-invalidation) is also a failover slot.

The standbys must synchronize failover slots before they can be used after promotion (`sync_replication_slots = on` and `hot_standby_feedback = on` on the standby, and the standby's physical slot in `synchronized_standby_slots` on the primary). Managed Postgres providers usually expose these as cluster parameters.

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

## Metrics

Prometheus metrics are served at `/metrics` on `[::]:9000`. Set `PGSTREAM_METRICS_ADDR` to listen elsewhere, for example to run several instances on one host network:

```bash
PGSTREAM_METRICS_ADDR=127.0.0.1:9187 postgres-stream
```
