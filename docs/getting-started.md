# Getting Started

## Requirements

- **Postgres 15+** with `wal_level=logical`
- User with `REPLICATION` privilege

Check your setting:
```sql
SHOW wal_level;
```

Enable if needed (requires restart):
```sql
ALTER SYSTEM SET wal_level = logical;
```

## Configure

Create `config.yaml`:

```yaml
stream:
  id: 1
  pg_connection:
    host: localhost
    port: 5432
    name: mydb
    username: postgres
    password: postgres
    tls:
      enabled: false
  batch:
    max_size: 1000
    max_fill_secs: 5

sink:
  type: webhook
  url: https://httpbin.org/post
```

## Run

```bash
docker run -v $(pwd)/config.yaml:/config.yaml \
  ghcr.io/psteinroe/postgres-stream:webhook-latest
```

Each sink has its own image: `kafka-latest`, `nats-latest`, `sqs-latest`, etc.

## Install the Optional Subscription Package

Subscriptions are user-managed SQL and are not installed by the pgstream daemon. Copy the provided migration into your application's migration system and run it as the role that owns your subscribed tables:

```bash
psql "$DATABASE_URL" \
  --set ON_ERROR_STOP=1 \
  --single-transaction \
  --file extensions/subscriptions/migrations/0001_create_pgstream_subscriptions.sql
```

See [Subscription Setup](guides/subscription-setup.md) for ownership, grants, and customization.

## Create a Subscription

```sql
INSERT INTO pgstream_subscriptions.subscriptions (
  key, stream_id, operation, schema_name, table_name, column_names
)
VALUES (
  'user-created', 1, 'INSERT', 'public', 'users', array['id', 'email']
);
```

Now inserts into `users` are streamed to your webhook.

## Next Steps

- [Subscription Setup](guides/subscription-setup.md) - Install the optional SQL package
- [Subscriptions](concepts/subscriptions.md) - Filter events, select columns
- [Sinks](sinks/index.md) - Configure your destination
- [Configuration Reference](reference/configuration.md) - All options
