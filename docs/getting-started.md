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
    memory_budget_ratio: 0.2
    max_fill_ms: 5000

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

## Create a Subscription

```sql
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name)
VALUES ('user-created', 1, 'INSERT', 'public', 'users');
```

Now inserts into `users` are streamed to your webhook.

## Next Steps

- [Subscriptions](concepts/subscriptions.md) - Filter events, select columns
- [Sinks](sinks/index.md) - Configure your destination
- [Configuration Reference](reference/configuration.md) - All options
