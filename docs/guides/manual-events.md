# Manual Event Insertion

Insert events directly without using subscriptions.

## Overview

While subscriptions are the primary way to capture events, you can also insert events directly into the `pgstream.events` table. This is useful for:

- Custom events not tied to table changes
- Background job notifications
- Events from external sources
- Testing and debugging

## Basic Usage

```sql
insert into pgstream.events (payload, stream_id)
values (
  '{"type": "job-completed", "job_id": 123, "result": "success"}'::jsonb,
  1  -- Stream ID from config
);
```

## Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `payload` | jsonb | The event data |
| `stream_id` | bigint | Must match the `stream.id` in your config |

## Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `metadata` | jsonb | Routing information (topic, partition key, etc.) |

The `id` and `created_at` fields are auto-generated.

## With Metadata

Use metadata for routing:

```sql
insert into pgstream.events (payload, stream_id, metadata)
values (
  '{"type": "background-job", "job_id": 123}'::jsonb,
  1,
  '{"topic": "background-jobs", "priority": "high"}'::jsonb
);
```

## Examples

### Background Job Completion

```sql
insert into pgstream.events (payload, stream_id, metadata)
values (
  jsonb_build_object(
    'type', 'job-completed',
    'job_id', 123,
    'result', 'success',
    'duration_ms', 1500,
    'completed_at', now()
  ),
  1,
  '{"topic": "jobs"}'::jsonb
);
```

### System Alert

```sql
insert into pgstream.events (payload, stream_id, metadata)
values (
  jsonb_build_object(
    'type', 'alert',
    'severity', 'warning',
    'message', 'High memory usage detected',
    'host', 'server-1'
  ),
  1,
  '{"topic": "alerts", "priority": "high"}'::jsonb
);
```

### Batch Import

```sql
insert into pgstream.events (payload, stream_id, metadata)
select
  jsonb_build_object(
    'type', 'import-record',
    'record_id', id,
    'data', data
  ),
  1,
  '{"topic": "imports"}'::jsonb
from import_staging_table;
```

## Event Flow

Manually inserted events follow the same flow as trigger-generated events:

1. **Inserted** into `pgstream.events`
2. **Captured** via logical replication
3. **Delivered** to your configured sink

The only difference is they don't have the trigger metadata (`tg_name`, `tg_op`, etc.).

## Payload Structure

You can use any JSON structure. The payload is delivered directly to the sink without modification.

However, for consistency with trigger events, you might want to include:

```sql
jsonb_build_object(
  'type', 'custom-event',      -- Event type identifier
  'timestamp', extract(epoch from now()) * 1000,
  'data', your_data            -- Your event data
)
```

## Combining with Subscriptions

Manual events and subscription-triggered events can coexist in the same stream. They're processed in order based on insertion time.

Use different metadata keys to route them to different destinations:

```sql
-- Manual event to specific topic
metadata = '{"topic": "manual-events"}'

-- Subscription routes to different topic
metadata_extensions = '[{"json_path": "topic", "expression": "''table-events''"}]'
```
