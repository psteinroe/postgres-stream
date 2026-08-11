# Event Format Reference

JSON structure of events delivered to sinks.

## Event Envelope

Events from subscriptions have this structure:

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

## Fields

### `tg_name`

| | |
|--|--|
| Type | string |

The subscription `key` that matched this event.

### `tg_op`

| | |
|--|--|
| Type | string |
| Values | `INSERT`, `UPDATE`, `DELETE` |

The database operation that triggered the event.

### `tg_table_name`

| | |
|--|--|
| Type | string |

Name of the table where the operation occurred.

### `tg_table_schema`

| | |
|--|--|
| Type | string |

Schema of the table where the operation occurred.

### `timestamp`

| | |
|--|--|
| Type | integer |

Unix timestamp in milliseconds when the event was created.

### `new`

| | |
|--|--|
| Type | object or null |

The new row values. Present for `INSERT` and `UPDATE` operations.

Contains the columns specified in `column_names`.

### `old`

| | |
|--|--|
| Type | object or null |

The previous row values. Present for `UPDATE` and `DELETE` operations.

Contains the columns specified in `column_names`.

## Operation-Specific Structure

### INSERT

```json
{
  "tg_op": "INSERT",
  "new": { /* new row */ },
  "old": null
}
```

### UPDATE

```json
{
  "tg_op": "UPDATE",
  "new": { /* new row */ },
  "old": { /* previous row */ }
}
```

### DELETE

```json
{
  "tg_op": "DELETE",
  "new": null,
  "old": { /* deleted row */ }
}
```

## Payload Extensions

Fields from `payload_extensions` are added at the top level:

```json
{
  "tg_name": "order-created",
  "tg_op": "INSERT",
  "new": { "id": 456, "total": 99.99 },
  "date": "2024-12-12",
  "total_formatted": "$99.99"
}
```

Nested paths create nested objects:

```json
{
  "tg_name": "user-action",
  "new": { "id": 123 },
  "context": {
    "user_id": "abc-123",
    "role": "admin"
  }
}
```

## Manual Events

Events inserted directly into `pgstream.events` have whatever structure you provide in the `payload` column:

```json
{
  "type": "custom-event",
  "data": { "key": "value" }
}
```

They don't have trigger metadata unless you include it.

## Metadata vs Payload

**Payload** is the event data delivered to the sink:
- Stored in `pgstream.events.payload`
- Sent to the destination as the message body

**Metadata** controls routing and delivery:
- Stored in `pgstream.events.metadata`
- Read by sinks to determine topic, queue, index, etc.
- Not typically included in the delivered message body

## Data Type Mapping

Postgres types are serialized to JSON:

| Postgres | JSON |
|------------|------|
| integer, bigint | number |
| numeric, decimal | number |
| boolean | boolean |
| text, varchar | string |
| timestamp, date | string (ISO 8601) |
| json, jsonb | object/array |
| array | array |
| uuid | string |

## Example Events

### User Signup

```json
{
  "tg_name": "user-signup",
  "tg_op": "INSERT",
  "tg_table_name": "users",
  "tg_table_schema": "public",
  "timestamp": 1703001234567,
  "new": {
    "id": 1,
    "email": "user@example.com",
    "email_verified": true,
    "created_at": "2024-12-12T10:30:00Z"
  },
  "old": null
}
```

### Order Status Change

```json
{
  "tg_name": "order-status-changed",
  "tg_op": "UPDATE",
  "tg_table_name": "orders",
  "tg_table_schema": "public",
  "timestamp": 1703001234567,
  "new": {
    "id": 456,
    "status": "shipped",
    "updated_at": "2024-12-12T14:30:00Z"
  },
  "old": {
    "id": 456,
    "status": "pending",
    "updated_at": "2024-12-12T10:30:00Z"
  }
}
```

### User Deleted

```json
{
  "tg_name": "user-deleted",
  "tg_op": "DELETE",
  "tg_table_name": "users",
  "tg_table_schema": "public",
  "timestamp": 1703001234567,
  "new": null,
  "old": {
    "id": 1,
    "email": "user@example.com"
  }
}
```
