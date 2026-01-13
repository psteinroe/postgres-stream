# Payload Extensions

Add computed fields to event payloads using SQL expressions.

## Overview

Payload extensions let you add derived values to your event payloads. These are computed at trigger time using SQL expressions.

## Basic Usage

```sql
insert into pgstream.subscriptions (
  key, stream_id, operation, schema_name, table_name,
  column_names, payload_extensions
) values (
  'order-notification',
  1,
  'INSERT',
  'public',
  'orders',
  array['id', 'user_id', 'total'],
  '[
    {"json_path": "order_date", "expression": "new.created_at::date::text"},
    {"json_path": "total_formatted", "expression": "''$'' || new.total::text"}
  ]'::jsonb
);
```

## Extension Format

Each extension is a JSON object with:

| Field | Type | Description |
|-------|------|-------------|
| `json_path` | string | Where to place the value in the payload |
| `expression` | string | SQL expression to compute the value |

## Result

The above subscription produces events like:

```json
{
  "tg_name": "order-notification",
  "tg_op": "INSERT",
  "tg_table_name": "orders",
  "tg_table_schema": "public",
  "timestamp": 1703001234567,
  "new": {
    "id": 456,
    "user_id": 123,
    "total": 99.99
  },
  "order_date": "2024-12-12",
  "total_formatted": "$99.99"
}
```

## Common Use Cases

### Computed Fields

```json
[
  {"json_path": "date_only", "expression": "new.created_at::date::text"},
  {"json_path": "year", "expression": "extract(year from new.created_at)::int"}
]
```

### Formatted Values

```json
[
  {"json_path": "price_display", "expression": "''$'' || new.price::text"},
  {"json_path": "name_upper", "expression": "upper(new.name)"}
]
```

### Context Information

Add session or auth context to events:

```json
[
  {"json_path": "user_id", "expression": "auth.uid()::text"},
  {"json_path": "tenant_id", "expression": "current_setting(''app.tenant_id'', true)"}
]
```

### Nested Paths

Use dot notation to create nested objects:

```json
[
  {"json_path": "context.user_id", "expression": "auth.uid()::text"},
  {"json_path": "context.role", "expression": "auth.role()"}
]
```

Produces:

```json
{
  "context": {
    "user_id": "d0c12345-abcd-1234-efgh-567890abcdef",
    "role": "authenticated"
  }
}
```

## Expression Rules

- Expressions are SQL and run in the trigger context
- Use `new` to reference the new row (INSERT/UPDATE)
- Use `old` to reference the previous row (UPDATE/DELETE)
- String literals need double single quotes: `''literal''`
- Expressions must return a value that can be cast to JSONB

## Payload vs Metadata Extensions

Use **payload extensions** for data that should be in the event body:

- Computed fields the consumer needs
- Formatted display values
- Context information for processing

Use **metadata extensions** for routing and delivery configuration:

- Topic names
- Partition keys
- Queue names

See [Event Metadata](event-metadata.md) for more on metadata extensions.

## Next Steps

- [Event Metadata](event-metadata.md) - Configure routing with metadata
- [Subscriptions](subscriptions.md) - Full subscription reference
