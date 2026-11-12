# Configuration Reference

Complete reference for the configuration options. See [Loading Configuration](#loading-configuration) for where they are read from.

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
    max_fill_ms: 5000
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
| Default | 100000 |

Maximum events per batch. Larger batches improve throughput but increase latency.

#### `max_fill_ms`

| | |
|--|--|
| Type | integer |
| Required | No |
| Default | 10000 |

Maximum time to fill a batch in milliseconds. Lower values reduce latency but may result in smaller batches.

### `stream.failover`

| | |
|--|--|
| Type | boolean |
| Required | No |
| Default | false |

Make the replication slot a [failover slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION) so it survives a switchover or failover to a standby. Requires Postgres 17 or newer; with an older server the stream fails to start when this is enabled.

When enabled, a new slot is created with `FAILOVER`, and an existing slot is altered to be a failover slot on startup, without resyncing. A slot recreated after [slot loss](../concepts/failover.md#slot-loss) is also a failover slot.

The standbys must synchronize failover slots before they can be used after promotion (`sync_replication_slots = on` and `hot_standby_feedback = on` on the standby, and the standby's physical slot in `synchronized_standby_slots` on the primary). Managed Postgres providers usually expose these as cluster parameters. On startup, the stream logs a warning if `synchronized_standby_slots` is empty on the server, but still starts, since standbys may be managed by the provider. See [Database Failover](../concepts/failover.md#database-failover).

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

## Loading Configuration

Postgres Stream reads its configuration from a directory, merging three sources in order (later ones win):

1. `base.yaml`, `base.yml` or `base.json` (required)
2. `<environment>.yaml`, `.yml` or `.json` (optional), where `<environment>` is `APP_ENVIRONMENT`: `prod` (default), `staging` or `dev`
3. Environment variables prefixed with `APP_`

The directory is `APP_CONFIG_DIR` if set, otherwise `configuration` in the current directory. The Docker images run from `/app` and ship a minimal `/app/configuration/base.yml`, so mount your own directory over it or set `APP_CONFIG_DIR`:

```bash
docker run -v $(pwd)/configuration:/app/configuration \
  ghcr.io/psteinroe/postgres-stream:kafka-latest
```

### Environment Variables

Every option can be set with an environment variable: prefix the path with `APP_` and separate nested keys with `__` (double underscore). Values are parsed as numbers and booleans where possible. Use this for secrets instead of writing them into the files:

```bash
APP_STREAM__PG_CONNECTION__PASSWORD=secret
APP_STREAM__BATCH__MAX_FILL_MS=1000
APP_SINK__TYPE=kafka
```

Configuration files do not expand `${VAR}` references.

## Metrics

Prometheus metrics are served at `/metrics` on `[::]:9000`. Set `PGSTREAM_METRICS_ADDR` to listen elsewhere, for example to run several instances on one host network:

```bash
PGSTREAM_METRICS_ADDR=127.0.0.1:9187 postgres-stream
```
