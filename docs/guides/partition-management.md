# Partition Management

Understanding and monitoring the partitioned events table.

## Overview

Postgres Stream stores events in a partitioned table (`pgstream.events`) with daily partitions. This keeps the table fast and manageable while providing a recovery window for failover scenarios.

## Default Behavior

Postgres Stream automatically manages partitions:

- **Creates** partitions 7 days in advance
- **Drops** partitions older than 7 days
- **Runs** on startup and daily thereafter

This provides a 7-day retention window for event replay during failover.

## Partition Naming

Partitions are named by date:

```
pgstream.events_20250110
pgstream.events_20250111
pgstream.events_20250112
...
```

## Viewing Partitions

List all event partitions:

```sql
select
  child.relname as partition_name,
  pg_size_pretty(pg_total_relation_size(child.oid)) as size
from pg_inherits
join pg_class parent on pg_inherits.inhparent = parent.oid
join pg_class child on pg_inherits.inhrelid = child.oid
where parent.relname = 'events'
  and parent.relnamespace = (select oid from pg_namespace where nspname = 'pgstream')
order by child.relname;
```

Expected output (14 partitions: 7 past + 7 future):

```
     partition_name     |  size
------------------------+---------
 events_20250106        | 48 kB
 events_20250107        | 256 kB
 events_20250108        | 1024 kB
 events_20250109        | 512 kB
 events_20250110        | 128 kB
 events_20250111        | 8192 bytes
 events_20250112        | 8192 bytes
 events_20250113        | 8192 bytes
 ...
```

## Monitoring

### Total Events Table Size

```sql
select pg_size_pretty(pg_total_relation_size('pgstream.events'));
```

### Events Per Partition

```sql
select
  tableoid::regclass as partition,
  count(*) as event_count
from pgstream.events
group by tableoid
order by tableoid::regclass::text;
```

### Event Volume Over Time

```sql
select
  date_trunc('hour', created_at) as hour,
  count(*) as events
from pgstream.events
where created_at > now() - interval '24 hours'
group by 1
order by 1;
```

## High Volume Considerations

For high-volume systems:

1. **Monitor partition size** - Large partitions may indicate high event volume
2. **Check disk space** - Ensure sufficient space for 7 days of events
3. **Watch processing rate** - Events should be processed faster than generated

### Disk Space Estimate

Estimate required disk space:

```
Daily events × 7 days × Average event size
```

Example:
- 1 million events/day
- ~500 bytes/event average
- 7 days retention
- = ~3.5 GB minimum

## Manual Partition Operations

While automatic management handles most cases, you can manually manage partitions if needed.

### Create a Partition

```sql
create table pgstream.events_20250120
  partition of pgstream.events
  for values from ('2025-01-20') to ('2025-01-21');
```

### Drop a Partition

```sql
drop table pgstream.events_20250106;
```

### Detach Without Dropping

To archive a partition:

```sql
alter table pgstream.events detach partition pgstream.events_20250106;
-- Now events_20250106 is a standalone table
```

## Troubleshooting

### Missing Partitions

If partitions are missing, Postgres Stream creates them on startup. Restart the service or wait for the daily maintenance run.

### Partitions Not Being Dropped

Check that Postgres Stream is running. Partitions are only dropped during the maintenance cycle.

### Large Partition Sizes

Large partitions may indicate:
- High event volume (expected)
- Events not being processed (check sink health)
- Accumulating during failover (check for checkpoint)

### Out of Disk Space

If disk is full:
1. Check if old partitions are being dropped
2. Manually drop oldest partitions if needed
3. Consider reducing batch size or increasing processing capacity
