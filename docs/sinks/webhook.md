# Webhook

Deliver events via HTTP POST requests.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:webhook-latest
```

## Configuration

```yaml
sink:
  type: webhook
  url: https://api.example.com/events
```

### With Headers

```yaml
sink:
  type: webhook
  url: https://api.example.com/events
  headers:
    Authorization: Bearer ${API_TOKEN}
    X-Custom-Header: value
  timeout_ms: 30000
```

## Options

| Option | Type | Required | Default | Metadata Override | Description |
|--------|------|----------|---------|-------------------|-------------|
| `url` | string | No | - | Yes | Default URL (can be overridden per-event) |
| `headers` | object | No | {} | Yes | Custom headers to include |
| `timeout_ms` | integer | No | 30000 | No | Request timeout in milliseconds |

## Dynamic Routing

Route events to different URLs using metadata:

```sql
-- Route by tenant
metadata_extensions = '[
  {"json_path": "url", "expression": "''https://api.example.com/'' || new.tenant_id"}
]'
```

Add headers dynamically:

```sql
-- Add tenant header
metadata = '{"headers": {"X-Tenant-Id": "tenant-123"}}'
```

The sink reads `url` and `headers` from event metadata.

## Request Format

Events are sent as JSON with:
- Method: POST
- Content-Type: `application/json`
- Body: Array of event payloads

When multiple events target the same URL, they're batched into a single request:

```json
[
  {"id": 1, "name": "event1"},
  {"id": 2, "name": "event2"}
]
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
  type: webhook
  url: https://httpbin.org/post
  headers:
    Authorization: Bearer secret-token
  timeout_ms: 30000
```

The sink expects a 2xx response. Non-success status codes cause the batch to fail and retry.
