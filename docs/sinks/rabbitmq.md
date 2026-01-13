# RabbitMQ

Enterprise message broker with RabbitMQ.

## Docker Image

```bash
docker pull ghcr.io/psteinroe/postgres-stream:rabbitmq-latest
```

## Configuration

```yaml
sink:
  type: rabbitmq
  url: amqp://guest:guest@localhost:5672
  exchange: events
  routing_key: postgres.events
```

### With Queue Binding

```yaml
sink:
  type: rabbitmq
  url: amqp://guest:guest@localhost:5672
  exchange: events
  routing_key: postgres.events
  queue: events-queue
```

## Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | string | Yes | - | RabbitMQ connection URL |
| `exchange` | string | No | - | Exchange name (can be overridden per-event) |
| `routing_key` | string | No | - | Routing key (can be overridden per-event) |
| `queue` | string | No | - | Optional queue to declare and bind |

## Dynamic Routing

Route events using metadata:

```sql
-- Dynamic exchange and routing key
metadata_extensions = '[
  {"json_path": "exchange", "expression": "''events''"},
  {"json_path": "routing_key", "expression": "tg_table_name || ''.'' || tg_op"}
]'
```

The sink reads `exchange` and `routing_key` from event metadata.

## Exchange Setup

The sink declares the exchange as a **topic exchange** with:
- Durable: true
- Auto-delete: false

If a queue is specified, it's declared and bound to the exchange with the routing key.

## Example

Complete configuration:

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
  type: rabbitmq
  url: amqp://guest:guest@localhost:5672
  exchange: postgres-events
  routing_key: events.#
  queue: events-consumer
```

Messages are published with:
- Content-Type: `application/json`
- Delivery mode: Persistent (2)
