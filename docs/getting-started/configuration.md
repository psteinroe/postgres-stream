# Configuration

Complete guide to configuring Postgres Stream.

## Configuration File

Postgres Stream uses a YAML configuration file. By default, it looks for `config.yaml` in the current directory.

## Minimal Configuration

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
  type: memory  # Built-in test sink
```

## Stream Configuration

### Connection Settings

```yaml
stream:
  id: 1  # Unique identifier for this stream
  pg_connection:
    host: localhost
    port: 5432
    name: mydb
    username: postgres
    password: postgres
    tls:
      enabled: false
      trusted_root_certs: ""  # Path to CA certificate
    keepalive: 60  # Optional: TCP keepalive in seconds
```

### Batch Settings

Control how events are grouped before delivery:

```yaml
stream:
  batch:
    max_size: 1000      # Maximum events per batch
    max_fill_secs: 5    # Maximum time to fill a batch (seconds)
```

- **max_size**: Larger batches improve throughput but increase latency
- **max_fill_secs**: Lower values reduce latency but may result in smaller batches

## Sink Configuration

The `sink` section defines where events are delivered. Each sink type has different options.

### Example: Kafka

```yaml
sink:
  type: kafka
  brokers: localhost:9092
  topic: events
```

### Example: Webhook

```yaml
sink:
  type: webhook
  url: https://api.example.com/events
  headers:
    Authorization: Bearer token123
  timeout_ms: 30000
```

See the [Sinks](../sinks/index.md) section for all available sinks and their configuration options.

## Environment Variables

Sensitive values can be provided via environment variables:

```yaml
stream:
  pg_connection:
    password: ${POSTGRES_PASSWORD}

sink:
  type: kafka
  sasl_password: ${KAFKA_PASSWORD}
```

## TLS Configuration

For secure connections:

```yaml
stream:
  pg_connection:
    tls:
      enabled: true
      trusted_root_certs: /path/to/ca.crt
```

## Full Reference

See [Configuration Reference](../reference/configuration.md) for all available options.
