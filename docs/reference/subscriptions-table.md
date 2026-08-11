# Subscriptions Table Reference

Schema reference for `pgstream_subscriptions.subscriptions`.

## Schema

```sql
create table pgstream_subscriptions.subscriptions (
  id uuid primary key default pgstream.portable_uuidv7(),
  key text not null,
  stream_id bigint,
  operation pgstream_subscriptions.operation_type not null,
  schema_name text not null,
  table_name text not null,
  when_clause text,
  column_names text[] not null,
  metadata jsonb,
  payload_extensions jsonb default '[]'::jsonb,
  metadata_extensions jsonb default '[]'::jsonb,
  unique (stream_id, key, schema_name, table_name, operation)
);
```

## Columns

### `id`

| | |
|--|--|
| Type | uuid |
| Required | Generated automatically |
| Primary Key | Yes |

Stable identifier generated with `pgstream.portable_uuidv7()`.

### `key`

| | |
|--|--|
| Type | text |
| Required | Yes |
| Unique | With stream, schema, table, and operation |

Identifier for the subscription. Used as `tg_name` in generated events.

### `stream_id`

| | |
|--|--|
| Type | bigint |
| Required | Recommended |
| Primary Key | No |

Must match the `stream.id` in your config. Links the subscription to a specific Postgres Stream instance.

### `operation`

| | |
|--|--|
| Type | text |
| Required | Yes |
| Values | `INSERT`, `UPDATE`, `DELETE` |

The database operation to capture.

### `schema_name`

| | |
|--|--|
| Type | text |
| Required | Yes |

Database schema containing the target table.

### `table_name`

| | |
|--|--|
| Type | text |
| Required | Yes |

Name of the table to monitor.

### `when_clause`

| | |
|--|--|
| Type | text |
| Required | No |

SQL expression to filter events. Use `new` for new values and `old` for previous values.

Examples:
```sql
'new.email_verified = true'
'old.status IS DISTINCT FROM new.status'
'new.amount > 1000'
```

### `column_names`

| | |
|--|--|
| Type | text[] |
| Required | Yes |

Array of column names to include in the event payload.

Example:
```sql
array['id', 'email', 'created_at']
```

### `metadata`

| | |
|--|--|
| Type | jsonb |
| Required | No |

Static metadata for routing. Applied to every event from this subscription.

Example:
```sql
'{"topic": "user-events", "priority": "high"}'::jsonb
```

### `payload_extensions`

| | |
|--|--|
| Type | jsonb |
| Required | No |

Array of computed fields to add to the event payload.

Format:
```json
[
  {"json_path": "field_name", "expression": "SQL expression"}
]
```

Example:
```sql
'[
  {"json_path": "date", "expression": "new.created_at::date::text"},
  {"json_path": "total_formatted", "expression": "''$'' || new.total::text"}
]'::jsonb
```

### `metadata_extensions`

| | |
|--|--|
| Type | jsonb |
| Required | No |

Array of computed metadata fields. Same format as `payload_extensions`.

Example:
```sql
'[
  {"json_path": "topic", "expression": "''events-'' || tg_table_name"},
  {"json_path": "partition_key", "expression": "new.user_id::text"}
]'::jsonb
```

## Extension Format

Both `payload_extensions` and `metadata_extensions` use the same format:

```json
[
  {
    "json_path": "path.to.field",
    "expression": "SQL expression"
  }
]
```

### `json_path`

Dot notation path where the value will be placed. Supports nesting:
- `"field"` → `{"field": value}`
- `"nested.field"` → `{"nested": {"field": value}}`

### `expression`

SQL expression evaluated in trigger context:
- Use `new` to reference new row values
- Use `old` to reference old row values
- String literals need double single quotes: `''literal''`
- Any valid SQL expression that returns a JSONB-compatible value

## Trigger Behavior

When you insert, update, or delete a subscription:

1. Postgres Stream's trigger functions recalculate
2. New database triggers are created/modified on target tables
3. Existing triggers are dropped if no subscriptions remain

This happens through internal triggers installed by the optional subscription SQL package. The package is owned by your database migration role; the pgstream daemon does not perform this DDL.
