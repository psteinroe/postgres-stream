# Subscriptions

Define which table changes to capture and how they should be formatted.

## Overview

Subscriptions are provided by an [optional, user-installed SQL package](../guides/subscription-setup.md) and stored in `pgstream_subscriptions.subscriptions`. When you insert, update, or delete a subscription, the package drops and recreates the corresponding database triggers. The pgstream daemon does not manage these triggers.

## Creating a Subscription

```sql
insert into pgstream_subscriptions.subscriptions (
  key,
  stream_id,
  operation,
  schema_name,
  table_name,
  when_clause,
  column_names,
  payload_extensions
) values (
  'user-signup',                          -- Unique identifier
  1,                                      -- Stream ID from config
  'INSERT',                               -- Operation: INSERT, UPDATE, or DELETE
  'public',                               -- Schema name
  'users',                                -- Table name
  'new.email_verified = true',            -- Optional filter (SQL expression)
  array['id', 'email', 'created_at'],     -- Columns to include in payload
  '[]'::jsonb                             -- Payload extensions (see below)
);
```

## Subscription Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key` | text | Yes | Unique identifier for the subscription |
| `stream_id` | bigint | Yes | Must match the `stream.id` in your config |
| `operation` | text | Yes | `INSERT`, `UPDATE`, or `DELETE` |
| `schema_name` | text | Yes | Database schema (usually `public`) |
| `table_name` | text | Yes | Target table name |
| `when_clause` | text | No | SQL expression to filter events |
| `column_names` | text[] | Yes | Columns to include in the payload |
| `payload_extensions` | jsonb | No | Computed fields to add to payload |
| `metadata` | jsonb | No | Static routing metadata |
| `metadata_extensions` | jsonb | No | Dynamic routing metadata |

## Filtering Events

Use `when_clause` to capture only specific events:

```sql
-- Only capture high-value orders
insert into pgstream_subscriptions.subscriptions (key, stream_id, operation, schema_name, table_name, when_clause, column_names)
values ('high-value-orders', 1, 'INSERT', 'public', 'orders', 'new.total > 1000', array['id', 'total']);

-- Only capture status changes
insert into pgstream_subscriptions.subscriptions (key, stream_id, operation, schema_name, table_name, when_clause, column_names)
values ('status-changed', 1, 'UPDATE', 'public', 'orders', 'old.status IS DISTINCT FROM new.status', array['id', 'status']);
```

The `when_clause` is a SQL expression. Use `new` to reference the new row (INSERT/UPDATE) and `old` for the previous row (UPDATE/DELETE).

## Selecting Columns

Use `column_names` to select the columns included in the payload:

```sql
-- Only include id, email, and created_at
insert into pgstream_subscriptions.subscriptions (key, stream_id, operation, schema_name, table_name, column_names)
values ('user-created', 1, 'INSERT', 'public', 'users', array['id', 'email', 'created_at']);
```

## Event Output

When a subscription matches, the event looks like:

```json
{
  "tg_name": "user-signup",
  "tg_op": "INSERT",
  "tg_table_name": "users",
  "tg_table_schema": "public",
  "timestamp": 1703001234567,
  "new": {
    "id": 123,
    "email": "user@example.com",
    "created_at": "2024-12-12T10:30:00Z"
  },
  "old": null
}
```

For UPDATE operations, both `new` and `old` are populated. For DELETE, only `old` is populated.

## Multiple Subscriptions per Table

You can have multiple subscriptions on the same table:

```sql
-- Capture all user inserts
insert into pgstream_subscriptions.subscriptions (key, stream_id, operation, schema_name, table_name, column_names)
values ('all-users', 1, 'INSERT', 'public', 'users', array['id', 'email', 'email_verified']);

-- Also capture verified users separately
insert into pgstream_subscriptions.subscriptions (key, stream_id, operation, schema_name, table_name, when_clause, column_names)
values ('verified-users', 1, 'INSERT', 'public', 'users', 'new.email_verified = true', array['id', 'email']);
```

Both subscriptions will fire for a verified user, creating two events with different `tg_name` values.

## Reconciling a Stream

Each changed subscription recreates its target trigger. The package includes an optional [`set_subscriptions()` example](https://github.com/psteinroe/postgres-stream/blob/main/extensions/subscriptions/examples/set_subscriptions.sql) that you can copy into your own migrations; it is not installed by default.

The helper accepts the complete desired state for one stream: missing rows are inserted, changed rows are updated, and omitted rows are deleted. Unchanged rows are not written, avoiding unnecessary trigger recreation.

## Next Steps

- [Event Structure](event-structure.md) - Payload and metadata format
- [Extensions](extensions.md) - Add computed fields and dynamic routing
