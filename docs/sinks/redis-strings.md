# Redis Strings

Store events as key-value pairs in Redis.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:redis-strings-latest
```

## Configuration

```yaml
sink:
  type: redis-strings
  url: redis://localhost:6379
```

### With Key Prefix

```yaml
sink:
  type: redis-strings
  url: redis://localhost:6379
  key_prefix: events
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `url` | string | Yes | - | No | Redis connection URL |
| `key_prefix` | string | No | - | No | Prefix for all keys |
| `key` | - | - | - | Yes | Full key (via metadata only) |

## Key Resolution

Keys are determined in this order:

1. `key` in event metadata
2. Event ID with optional prefix

Without a key in metadata:
- No prefix: key = event ID
- With prefix: key = `{prefix}:{event_id}`

## Dynamic Routing

Set custom keys per-event using metadata:

```sql
-- Use user_id as the key
metadata_extensions = '[
  {"json_path": "key", "expression": "''user:'' || new.user_id::text"}
]'
```

The sink reads the `key` from event metadata.

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
  type: redis-strings
  url: redis://localhost:6379
  key_prefix: postgres
```

Values are stored as JSON-serialized payloads using Redis `SET` command.
