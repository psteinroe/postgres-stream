# Event Structure

Events have two parts: **payload** (the data) and **metadata** (routing info).

## Payload

The event body sent to your sink. Contains the row data and trigger context.

```json
{
  "tg_name": "user-created",
  "tg_op": "INSERT",
  "tg_table_name": "users",
  "tg_table_schema": "public",
  "timestamp": 1703001234567,
  "new": {
    "id": 123,
    "email": "user@example.com"
  }
}
```

| Field | Description |
|-------|-------------|
| `tg_name` | Subscription key |
| `tg_op` | Operation: INSERT, UPDATE, DELETE |
| `tg_table_name` | Source table |
| `tg_table_schema` | Source schema |
| `timestamp` | Event time (Unix ms) |
| `new` | New row data (INSERT/UPDATE) |
| `old` | Previous row data (UPDATE/DELETE) |

### Selecting Columns

By default, all columns are included. Use `column_names` to select specific columns:

```sql
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, column_names)
VALUES ('user-created', 1, 'INSERT', 'public', 'users', ARRAY['id', 'email']);
```

## Metadata

Routing configuration read by sinks. Controls where and how events are delivered.

```sql
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, metadata)
VALUES ('user-created', 1, 'INSERT', 'public', 'users', '{"topic": "users", "priority": "high"}');
```

Each sink reads specific metadata fields:

| Sink | Fields |
|------|--------|
| Kafka | `topic` |
| NATS | `topic` |
| RabbitMQ | `exchange`, `routing_key` |
| Redis Strings | `key` |
| Redis Streams | `stream` |
| Webhook | `url`, `headers` |
| SQS | `queue_url` |
| SNS | `topic` |
| Kinesis | `stream` |
| Elasticsearch | `index` |
| Meilisearch | `index` |

See [Extensions](extensions.md) to compute payload and metadata values dynamically.
