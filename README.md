<div align="center">
  <img src="docs/media/logo.png" alt="Postgres Stream" width="300">

  # Postgres Stream
  Reliably stream Postgres table changes to external systems with automatic failover and zero event loss.

  [Documentation](https://pg-stream.dev/)
</div>


## Features

- **Single binary** - No complex infrastructure or high-availability destinations required
- **Postgres-native durability** - Events are stored in the database, WAL can be released immediately
- **Zero data loss** - As long as downtime is less than partition retention (7 days by default)
- **Automatic recovery** - Handles both sink failures and slot invalidation without operator intervention

## License

MIT
