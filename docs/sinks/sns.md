# AWS SNS

Publish events to Amazon SNS topics.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:sns-latest
```

## Configuration

```yaml
sink:
  type: sns
  topic_arn: arn:aws:sns:us-east-1:123456789:my-topic
  region: us-east-1
```

### With Explicit Credentials

```yaml
sink:
  type: sns
  topic_arn: arn:aws:sns:us-east-1:123456789:my-topic
  region: us-east-1
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### For Local Testing

```yaml
sink:
  type: sns
  topic_arn: arn:aws:sns:us-east-1:000000000000:my-topic
  region: us-east-1
  endpoint_url: http://localhost:4566
  access_key_id: local
  secret_access_key: local
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `topic_arn` | string | No | - | Yes | SNS topic ARN (can be overridden per-event) |
| `region` | string | Yes | - | No | AWS region |
| `endpoint_url` | string | No | - | No | Custom endpoint for LocalStack |
| `access_key_id` | string | No | - | No | AWS access key (uses default chain if not set) |
| `secret_access_key` | string | No | - | No | AWS secret key (uses default chain if not set) |

## Dynamic Routing

Route events to different topics using metadata:

```sql
-- Route by event type
metadata_extensions = '[
  {"json_path": "topic", "expression": "''arn:aws:sns:us-east-1:123456789:'' || new.event_type"}
]'
```

The sink reads `topic` from event metadata.

## Batching

Messages are published using `PublishBatch` with up to 10 messages per request (SNS limit). Multiple batches are sent concurrently.

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
  type: sns
  topic_arn: arn:aws:sns:us-east-1:123456789:postgres-events
  region: us-east-1
```

Messages are published as JSON-serialized payloads.
