# Getting Started

Get Postgres Stream running in minutes.

## Overview

Setting up Postgres Stream involves four steps:

1. **Check requirements** - Ensure your Postgres supports logical replication
2. **Configure** - Create a `config.yaml` with your connection and sink settings
3. **Run** - Start the binary or Docker container
4. **Create subscriptions** - Define which table changes to capture

## Quick Start

The fastest way to get started:

```bash
# 1. Create config.yaml
cat > config.yaml << 'EOF'
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
  url: https://webhook.site/your-unique-url
EOF

# 2. Run with Docker
docker run -v $(pwd)/config.yaml:/config.yaml \
  ghcr.io/psteinroe/postgres-stream:webhook-latest

# 3. Create a subscription in your database
psql -c "
INSERT INTO pgstream.subscriptions (key, stream_id, operation, schema_name, table_name, column_names)
VALUES ('user-created', 1, 'INSERT', 'public', 'users', ARRAY['id', 'email']);
"
```

Now when you insert into the `users` table, events are delivered to your webhook.

## Next Steps

- [Requirements](requirements.md) - Postgres version and configuration
- [Configuration](configuration.md) - Full config.yaml reference
- [Running](running.md) - Docker and binary options
