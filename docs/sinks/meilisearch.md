# Meilisearch

Index events as searchable documents in Meilisearch.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:meilisearch-latest
```

## Configuration

```yaml
sink:
  type: meilisearch
  url: http://localhost:7700
  index: events
```

### With Authentication

```yaml
sink:
  type: meilisearch
  url: http://localhost:7700
  index: events
  api_key: ${MEILISEARCH_API_KEY}
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `url` | string | Yes | - | No | Meilisearch URL |
| `index` | string | No | - | Yes | Default index (can be overridden per-event) |
| `api_key` | string | No | - | No | API key for authentication |

## Dynamic Routing

Route events to different indexes using metadata:

```sql
-- Route by table name
metadata_extensions = '[
  {"json_path": "index", "expression": "tg_table_name"}
]'

-- Or use static metadata
metadata = '{"index": "products"}'
```

The sink reads `index` from event metadata.

## Primary Key

Meilisearch requires each document to have a primary key. Options:

1. **Configure in Meilisearch**: Set the primary key field when creating the index
2. **Add via payload_extensions**: Transform the event ID into the expected field

```sql
-- Add id field from event
payload_extensions = '[
  {"json_path": "id", "expression": "new.id::text"}
]'
```

## Document Format

The event payload is indexed directly (not the full envelope). Your payload structure should match your Meilisearch schema.

## Task Handling

The sink waits for indexing tasks to complete before returning success. This ensures data consistency but may impact throughput for large batches.

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
  type: meilisearch
  url: http://localhost:7700
  index: products
  api_key: masterKey
```

## Index Setup

Create an index with appropriate settings:

```bash
curl -X POST 'http://localhost:7700/indexes' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer masterKey' \
  --data-binary '{
    "uid": "products",
    "primaryKey": "id"
  }'
```
