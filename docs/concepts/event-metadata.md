# Event Metadata

Configure routing and sink behavior using event metadata.

## Overview

Metadata controls how and where events are delivered. It's stored separately from the payload and read by sinks to determine routing (topic, queue, partition key, etc.).

There are two types:

- **Static metadata** - Same for every event from a subscription
- **Dynamic metadata** - Computed from row data using SQL expressions

## Static Metadata

Use the `metadata` column for values that don't change:

```sql
insert into pgstream.subscriptions (
  key, stream_id, operation, schema_name, table_name,
  column_names, metadata
) values (
  'user-events',
  1,
  'INSERT',
  'public',
  'users',
  array['id', 'email'],
  '{"topic": "user-events", "priority": "high"}'::jsonb
);
```

## Dynamic Metadata

Use `metadata_extensions` to compute values from row data:

```sql
insert into pgstream.subscriptions (
  key, stream_id, operation, schema_name, table_name,
  column_names, metadata_extensions
) values (
  'user-events',
  1,
  'INSERT',
  'public',
  'users',
  array['id', 'email'],
  '[
    {"json_path": "partition_key", "expression": "new.user_id::text"},
    {"json_path": "topic", "expression": "''users-'' || new.region"}
  ]'::jsonb
);
```

The format is identical to [payload extensions](payload-extensions.md).

## Combined Example

Use both together for flexible routing:

```sql
insert into pgstream.subscriptions (
  key, stream_id, operation, schema_name, table_name,
  column_names, metadata, metadata_extensions
) values (
  'user-events',
  1,
  'INSERT',
  'public',
  'users',
  array['id', 'email'],
  '{"priority": "high"}'::jsonb,
  '[
    {"json_path": "topic", "expression": "''users-'' || new.region"},
    {"json_path": "partition_key", "expression": "new.user_id::text"}
  ]'::jsonb
);
```

The resulting metadata merges static and dynamic values:

```json
{
  "priority": "high",
  "topic": "users-eu-west-1",
  "partition_key": "123"
}
```

## Nested Paths

Use dot notation to create nested objects:

```sql
'[
  {"json_path": "auth.user_id", "expression": "auth.uid()::text"},
  {"json_path": "auth.role", "expression": "auth.role()"}
]'::jsonb
```

Produces:

```json
{
  "auth": {
    "user_id": "d0c12345-abcd-1234-efgh-567890abcdef",
    "role": "authenticated"
  }
}
```

## Sink-Specific Metadata

Each sink reads specific metadata fields. Common patterns:

### Topic-Based Sinks (Kafka, NATS, etc.)

```json
{
  "topic": "events",
  "partition_key": "user-123"
}
```

### Queue-Based Sinks (SQS, RabbitMQ, etc.)

```json
{
  "queue": "events",
  "routing_key": "user.created"
}
```

### Search Sinks (Elasticsearch, Meilisearch)

```json
{
  "index": "users",
  "document_id": "123"
}
```

### Webhook Sink

```json
{
  "url": "https://api.example.com/events"
}
```

See each [sink's documentation](../sinks/index.md) for supported metadata fields.

## Expression Examples

### Using Auth Context (Supabase)

```json
[
  {"json_path": "user_id", "expression": "auth.uid()::text"},
  {"json_path": "role", "expression": "auth.role()"}
]
```

### Using Session Variables

```json
[
  {"json_path": "tenant_id", "expression": "current_setting(''app.tenant_id'', true)"}
]
```

### Derived From Row Data

```json
[
  {"json_path": "partition_key", "expression": "new.id::text"},
  {"json_path": "topic", "expression": "''orders-'' || new.region"}
]
```

## Next Steps

- [Sinks](../sinks/index.md) - See metadata fields for each sink
- [Payload Extensions](payload-extensions.md) - Add computed fields to the event body
