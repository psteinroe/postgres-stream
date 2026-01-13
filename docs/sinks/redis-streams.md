# Redis Streams

Append events to a Redis Stream for ordered event log.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:redis-streams-latest
```

## Configuration

```yaml
sink:
  type: redis-streams
  url: redis://localhost:6379
  stream_name: events
```

### With Length Limit

```yaml
sink:
  type: redis-streams
  url: redis://localhost:6379
  stream_name: events
  max_len: 100000
```

## Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | - | Redis connection URL |
| `stream_name` | string | No | - | Default stream (can be overridden per-event) |
| `max_len` | integer | No | - | Maximum stream length (uses MAXLEN ~) |

## Dynamic Routing

Route events to different streams using metadata:

```sql
-- Route by table name
metadata_extensions = '[
  {"json_path": "stream", "expression": "''events:'' || tg_table_name"}
]'
```

The sink reads the `stream` key from event metadata.

## Message Format

Events are added using `XADD` with:
- Auto-generated stream entry ID (`*`)
- Single field: `payload` containing JSON

Example when read with `XREAD`:
```text
1704067200000-0 payload {"id": 1, "name": "test"}
```

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
  type: redis-streams
  url: redis://localhost:6379
  stream_name: postgres-events
  max_len: 100000
```

The `max_len` option uses approximate trimming (`MAXLEN ~`) for efficiency.
