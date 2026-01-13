# Requirements

What you need to run Postgres Stream.

## Postgres Version

**Postgres 15 or higher** is required.

## Logical Replication

Your Postgres must have `wal_level=logical` enabled. This is required for change data capture.

### Check Current Setting

```sql
SHOW wal_level;
```

If it returns `replica` or `minimal`, you need to change it.

### Enable Logical Replication

For self-managed Postgres, add to `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Then restart Postgres.

### Cloud Providers

| Provider | How to Enable |
|----------|---------------|
| AWS RDS | Set `rds.logical_replication` parameter to `1` |
| Azure | Set `azure.logical_replication` to `ON` |
| GCP Cloud SQL | Enable `cloudsql.logical_decoding` flag |
| Supabase | Enabled by default |
| Neon | Enabled by default |

## User Permissions

The database user needs `REPLICATION` privilege:

```sql
ALTER USER myuser WITH REPLICATION;
```

Or create a dedicated user:

```sql
CREATE USER pgstream_user WITH REPLICATION PASSWORD 'secure_password';
GRANT ALL ON SCHEMA public TO pgstream_user;
GRANT ALL ON ALL TABLES IN SCHEMA public TO pgstream_user;
```

## Network Access

Ensure Postgres Stream can reach your database:

- Database port (default 5432) must be accessible
- If using TLS, ensure certificates are valid
- Firewall rules allow the connection

## Sink Requirements

Each sink has its own requirements. See the [Sinks](../sinks/index.md) section for details.
