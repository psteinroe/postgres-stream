# NATS

Lightweight pub/sub messaging with NATS.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:nats-latest
```

## Configuration

```yaml
sink:
  type: nats
  url: nats://localhost:4222
  subject: events
```

### With Authentication

```yaml
sink:
  type: nats
  url: nats://user:password@nats.example.com:4222
  subject: events
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `url` | string | Yes | - | No | NATS server URL |
| `subject` | string | No | - | Yes | Default subject (can be overridden per-event) |

## Dynamic Routing

Route events to different subjects using metadata:

```sql
-- Route by table name
metadata_extensions = '[
  {"json_path": "topic", "expression": "''events.'' || tg_table_name"}
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
    memory_budget_ratio: 0.2
    max_fill_ms: 5000

sink:
  type: nats
  url: nats://localhost:4222
  subject: postgres.events
```

Messages are published as JSON-serialized payloads.
