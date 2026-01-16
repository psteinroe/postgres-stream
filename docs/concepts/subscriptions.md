# Subscriptions

Define which table changes to capture and how they should be formatted.

## Overview

Subscriptions are stored in the `pgstream.subscriptions` table. When you insert, update, or delete a subscription, Postgres Stream automatically creates or updates the corresponding database triggers.

## Creating a Subscription

```sql
insert into pgstream.subscriptions (
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
| `column_names` | text[] | No | Columns to include in payload (null = all) |
| `payload_extensions` | jsonb | No | Computed fields to add to payload |
| `metadata` | jsonb | No | Static routing metadata |
| `metadata_extensions` | jsonb | No | Dynamic routing metadata |

## Filtering Events

Use `when_clause` to capture only specific events:

```sql
-- Only capture high-value orders
insert into pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, when_clause)
values ('high-value-orders', 1, 'INSERT', 'public', 'orders', 'new.total > 1000');

-- Only capture status changes
insert into pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, when_clause)
values ('status-changed', 1, 'UPDATE', 'public', 'orders', 'old.status IS DISTINCT FROM new.status');
```

The `when_clause` is a SQL expression. Use `new` to reference the new row (INSERT/UPDATE) and `old` for the previous row (UPDATE/DELETE).

## Selecting Columns

By default, all columns are included. Use `column_names` to select specific columns:

```sql
-- Only include id, email, and created_at
insert into pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, column_names)
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
insert into pgstream.subscriptions (key, stream_id, operation, schema_name, table_name)
values ('all-users', 1, 'INSERT', 'public', 'users');

-- Also capture verified users separately
insert into pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, when_clause)
values ('verified-users', 1, 'INSERT', 'public', 'users', 'new.email_verified = true');
```

Both subscriptions will fire for a verified user, creating two events with different `tg_name` values.

## Avoiding Unnecessary Trigger Recreation

Each subscription change recreates the trigger, which can be expensive. Use MERGE to only update when values actually change:

```sql
create or replace function set_subscriptions(
  p_stream_id bigint,
  p_subscriptions pgstream.subscriptions[]
)
returns void
language plpgsql
security definer
set search_path to ''
as $$
begin
  create temporary table temp_subscriptions as
  select * from unnest(p_subscriptions);

  -- Only update if values actually changed (avoids trigger recreation)
  merge into pgstream.subscriptions as target
  using temp_subscriptions as source
  on (target.key = source.key and target.stream_id = p_stream_id)
  when matched and (
    target.operation is distinct from source.operation or
    target.schema_name is distinct from source.schema_name or
    target.table_name is distinct from source.table_name or
    target.when_clause is distinct from source.when_clause or
    target.column_names is distinct from source.column_names or
    target.metadata is distinct from source.metadata or
    target.payload_extensions is distinct from source.payload_extensions or
    target.metadata_extensions is distinct from source.metadata_extensions
  ) then update set
    operation = source.operation,
    schema_name = source.schema_name,
    table_name = source.table_name,
    when_clause = source.when_clause,
    column_names = source.column_names,
    metadata = source.metadata,
    payload_extensions = source.payload_extensions,
    metadata_extensions = source.metadata_extensions
  when not matched then insert (
    key, stream_id, operation, schema_name, table_name,
    when_clause, column_names, metadata, payload_extensions, metadata_extensions
  ) values (
    source.key, p_stream_id, source.operation, source.schema_name,
    source.table_name, source.when_clause, source.column_names,
    source.metadata, source.payload_extensions, source.metadata_extensions
  );

  -- Remove subscriptions not in input
  delete from pgstream.subscriptions
  where stream_id = p_stream_id
    and not exists (
      select 1 from temp_subscriptions
      where pgstream.subscriptions.key = temp_subscriptions.key
    );

  drop table temp_subscriptions;
end;
$$;
```

## Next Steps

- [Event Structure](event-structure.md) - Payload and metadata format
- [Extensions](extensions.md) - Add computed fields and dynamic routing
