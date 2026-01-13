# AWS Kinesis

Stream events to Amazon Kinesis Data Streams.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:kinesis-latest
```

## Configuration

```yaml
sink:
  type: kinesis
  stream_name: my-stream
  region: us-east-1
```

### With Explicit Credentials

```yaml
sink:
  type: kinesis
  stream_name: my-stream
  region: us-east-1
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### For Local Testing

```yaml
sink:
  type: kinesis
  stream_name: my-stream
  region: us-east-1
  endpoint_url: http://localhost:4566
  access_key_id: local
  secret_access_key: local
```

## Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `stream_name` | string | No | - | Kinesis stream name (can be overridden per-event) |
| `region` | string | Yes | - | AWS region |
| `endpoint_url` | string | No | - | Custom endpoint for LocalStack |
| `access_key_id` | string | No | - | AWS access key (uses default chain if not set) |
| `secret_access_key` | string | No | - | AWS secret key (uses default chain if not set) |

## Dynamic Routing

Route events to different streams using metadata:

```sql
-- Route by region
metadata_extensions = '[
  {"json_path": "stream", "expression": "''events-'' || new.region"}
]'
```

The sink reads `stream` from event metadata.

## Partition Key

The event ID is used as the partition key for shard distribution.

## Batching

Records are sent using `PutRecords` with up to 500 records per request (Kinesis limit). Multiple batches are sent concurrently.

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
  type: kinesis
  stream_name: postgres-events
  region: us-east-1
```

Records are published with:
- Partition Key: Event ID
- Data: JSON-serialized payload
