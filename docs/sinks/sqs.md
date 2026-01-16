# AWS SQS

Send events to Amazon SQS queues.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:sqs-latest
```

## Configuration

```yaml
sink:
  type: sqs
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  region: us-east-1
```

### With Explicit Credentials

```yaml
sink:
  type: sqs
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  region: us-east-1
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### For Local Testing

```yaml
sink:
  type: sqs
  queue_url: http://localhost:9324/queue/my-queue
  region: us-east-1
  endpoint_url: http://localhost:9324
  access_key_id: local
  secret_access_key: local
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `queue_url` | string | No | - | Yes | SQS queue URL (can be overridden per-event) |
| `region` | string | Yes | - | No | AWS region |
| `endpoint_url` | string | No | - | No | Custom endpoint for LocalStack/ElasticMQ |
| `access_key_id` | string | No | - | No | AWS access key (uses default chain if not set) |
| `secret_access_key` | string | No | - | No | AWS secret key (uses default chain if not set) |

## Dynamic Routing

Route events to different queues using metadata:

```sql
-- Route by priority
metadata_extensions = '[
  {"json_path": "queue_url", "expression": "''https://sqs.us-east-1.amazonaws.com/123456789/'' || new.priority || ''-queue''"}
]'
```

The sink reads `queue_url` from event metadata.

## Batching

Messages are sent using `SendMessageBatch` with up to 10 messages per request (SQS limit). Multiple batches are sent concurrently.

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
  type: sqs
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/postgres-events
  region: us-east-1
```

Messages are sent as JSON-serialized payloads.
