# Elasticsearch

Index events as searchable documents in Elasticsearch.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:elasticsearch-latest
```

## Configuration

```yaml
sink:
  type: elasticsearch
  url: http://localhost:9200
  index: events
```

### With Authentication

```yaml
sink:
  type: elasticsearch
  url: http://elastic:password@localhost:9200
  index: events
```

## Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | - | Elasticsearch URL (may include credentials) |
| `index` | string | No | - | Default index (can be overridden per-event) |

## Dynamic Routing

Route events to different indexes using metadata:

```sql
-- Route by table name
metadata_extensions = '[
  {"json_path": "index", "expression": "tg_table_name || ''-events''"}
]'

-- Or use static metadata
metadata = '{"index": "users"}'
```

The sink reads `index` from event metadata.

## Document Format

Events are indexed with:
- Document ID: Event ID
- Body: Event payload (not the full envelope)

The payload is indexed directly, so your Elasticsearch mapping should match your payload structure.

## Bulk Operations

Events are indexed using the Bulk API for efficiency. All events in a batch are sent in a single bulk request.

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
  type: elasticsearch
  url: http://localhost:9200
  index: postgres-events
```

## Index Mapping

Create an index mapping that matches your event payload structure:

```json
PUT /postgres-events
{
  "mappings": {
    "properties": {
      "id": { "type": "keyword" },
      "email": { "type": "keyword" },
      "created_at": { "type": "date" }
    }
  }
}
```
