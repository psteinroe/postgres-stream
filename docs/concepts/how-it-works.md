# How It Works

Understanding the Postgres Stream architecture.

## Overview

Postgres Stream captures changes from your Postgres tables using a simple, database-native approach:

1. **You manage subscriptions** directly in the database (`pgstream.subscriptions` table)
2. **Subscription changes automatically create database triggers** on the target tables
3. **Application events fire the managed triggers**, which insert into a partitioned `events` table
4. **Postgres Stream streams events** via logical replication and delivers them to your sink

```mermaid
flowchart LR
    subgraph Postgres
        A[subscriptions<br/>table] -->|creates<br/>triggers| B[trigger<br/>on users]
        B -->|insert<br/>into| C[events<br/>partitioned<br/>20250112..]
        C -->|logical<br/>replication| D[replication<br/>slot]
    end
    D -->|streams| E[Postgres<br/>Stream]
    E -->|delivers| F[sink<br/>queue/http]

    style Postgres fill:#dbeafe,stroke:#2563eb,stroke-width:3px
    style A fill:#f3e8ff,stroke:#9333ea,stroke-width:2px,color:#581c87
    style B fill:#fed7aa,stroke:#ea580c,stroke-width:2px,color:#7c2d12
    style C fill:#bfdbfe,stroke:#2563eb,stroke-width:2px,color:#1e40af
    style D fill:#99f6e4,stroke:#14b8a6,stroke-width:2px,color:#115e59
    style E fill:#fecdd3,stroke:#e11d48,stroke-width:3px,color:#881337
    style F fill:#bbf7d0,stroke:#16a34a,stroke-width:3px,color:#14532d
```

## Why This Approach?

Traditional CDC tools read directly from the WAL, which creates operational challenges:

- WAL retention grows if the sink is slow or down
- Slot invalidation causes data loss
- Complex infrastructure for high availability

Postgres Stream uses a different model:

- Events are stored in a partitioned table in the database
- WAL can be released immediately after events are written
- The events table provides durability, not the WAL
- Recovery reads from the table, not the WAL

## Generated Triggers

When you insert a subscription, Postgres Stream creates a trigger function on the target table. Here's a simplified example:

```sql
-- Auto-generated when you insert into subscriptions table
create or replace function pgstream._publish_after_insert_on_users()
returns trigger as $$
declare
  v_jsonb_output jsonb := '[]'::jsonb;
  v_base_payload jsonb := jsonb_build_object(
    'tg_op', tg_op,
    'tg_table_name', tg_table_name,
    'tg_table_schema', tg_table_schema,
    'timestamp', (extract(epoch from now()) * 1000)::bigint
  );
begin
  -- Check subscription "user-signup" condition
  if new.email_verified = true then
    v_jsonb_output := v_jsonb_output || (jsonb_build_object(
      'tg_name', 'user-signup',
      'new', jsonb_build_object('id', new.id, 'email', new.email),
      'old', null
    ) || v_base_payload);
  end if;

  -- Write to events table if any subscriptions matched
  if jsonb_array_length(v_jsonb_output) > 0 then
    insert into pgstream.events (payload, stream_id)
    select elem, 1
    from jsonb_array_elements(v_jsonb_output) as t(elem);
  end if;

  return new;
end;
$$ language plpgsql;
```

The actual generated code handles multiple subscriptions per table, merges `when` clauses with OR logic, and includes all payload extensions.

## Event Flow

1. **Application writes** - Your application performs an INSERT, UPDATE, or DELETE
2. **Trigger fires** - The generated trigger evaluates subscription conditions
3. **Event written** - Matching events are inserted into `pgstream.events`
4. **Logical replication** - Postgres Stream receives the event via replication
5. **Sink delivery** - The event is delivered to your configured sink

## Partitioned Events Table

Events are stored in a partitioned table with daily partitions:

- **7-day retention** by default
- **Automatic partition creation** 7 days ahead
- **Automatic partition cleanup** for old data
- Partitioning keeps the table fast and manageable

## Next Steps

- [Subscriptions](subscriptions.md) - Define which events to capture
- [Payload Extensions](payload-extensions.md) - Add computed fields
- [Event Metadata](event-metadata.md) - Configure routing
