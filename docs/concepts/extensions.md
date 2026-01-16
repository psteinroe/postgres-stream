# Extensions

Add computed values to payload or metadata using SQL expressions.

## Payload Extensions

Add fields to the event body:

```sql
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, payload_extensions)
VALUES (
  'order-created', 1, 'INSERT', 'public', 'orders',
  '[
    {"json_path": "total_formatted", "expression": "''$'' || new.total::text"},
    {"json_path": "order_date", "expression": "new.created_at::date::text"}
  ]'
);
```

Result:

```json
{
  "new": {"id": 456, "total": 99.99},
  "total_formatted": "$99.99",
  "order_date": "2024-12-12"
}
```

## Metadata Extensions

Compute routing values from row data:

```sql
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, metadata_extensions)
VALUES (
  'order-created', 1, 'INSERT', 'public', 'orders',
  '[
    {"json_path": "topic", "expression": "''orders-'' || new.region"}
  ]'
);
```

Routes events to `orders-us-east-1`, `orders-eu-west-1`, etc. based on the row's region.

## Extension Format

| Field | Description |
|-------|-------------|
| `json_path` | Where to place the value (supports dot notation: `context.user_id`) |
| `expression` | SQL expression evaluated in trigger context |

## Available Variables

| Variable | Available | Description |
|----------|-----------|-------------|
| `new` | INSERT, UPDATE | New row data |
| `old` | UPDATE, DELETE | Previous row data |
| `tg_op` | Always | Operation name |
| `tg_table_name` | Always | Table name |
| `tg_table_schema` | Always | Schema name |

## Examples

### Dynamic Topic Routing

```sql
-- Route by table name
'[{"json_path": "topic", "expression": "''events-'' || tg_table_name"}]'

-- Route by row field
'[{"json_path": "topic", "expression": "''orders-'' || new.priority"}]'
```

### Add Auth Context

```sql
-- Supabase auth
'[
  {"json_path": "context.user_id", "expression": "auth.uid()::text"},
  {"json_path": "context.role", "expression": "auth.role()"}
]'

-- Session variables
'[{"json_path": "tenant_id", "expression": "current_setting(''app.tenant_id'', true)"}]'
```

### Computed Fields

```sql
'[
  {"json_path": "year", "expression": "extract(year from new.created_at)::int"},
  {"json_path": "full_name", "expression": "new.first_name || '' '' || new.last_name"}
]'
```

## Notes

- String literals need double single quotes: `''literal''`
- Expressions must return JSONB-compatible values
- Metadata extensions override static metadata values
