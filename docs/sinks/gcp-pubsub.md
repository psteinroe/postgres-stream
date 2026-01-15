# GCP Pub/Sub

Publish events to Google Cloud Pub/Sub.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:gcp-pubsub-latest
```

## Configuration

```yaml
sink:
  type: gcp-pubsub
  project_id: my-project
  topic: my-topic
```

### For Local Testing

```yaml
sink:
  type: gcp-pubsub
  project_id: my-project
  topic: my-topic
  emulator_host: localhost:8085
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `project_id` | string | Yes | - | No | GCP project ID |
| `topic` | string | Yes | - | No | Pub/Sub topic name |
| `emulator_host` | string | No | - | No | Emulator host for testing |

## Authentication

The sink uses the default GCP credentials chain:

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. Application Default Credentials (ADC)
3. GCE metadata service (when running on GCP)

## Emulator Support

For local testing with the Pub/Sub emulator:

1. Set `PUBSUB_EMULATOR_HOST` environment variable:
   ```bash
   export PUBSUB_EMULATOR_HOST=localhost:8085
   ```

2. Set `emulator_host` in config to enable auto-topic creation:
   ```yaml
   emulator_host: localhost:8085
   ```

## Dynamic Routing

Unlike other sinks, GCP Pub/Sub does not support dynamic topic routing via metadata. The topic must be specified in the sink configuration.

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
  type: gcp-pubsub
  project_id: my-gcp-project
  topic: postgres-events
```

Messages are published as JSON-serialized payloads. The Pub/Sub client handles batching internally (default: 10ms, 100 messages, or 1MiB).
