# Implementing Custom Sinks

Build your own sink to deliver events to any destination.

## Overview

Sinks implement the `Sink` trait to define how events are delivered. Each sink should be behind a feature flag to keep the binary minimal.

## Step 1: Add Dependencies

Update `Cargo.toml` with your sink's dependencies behind a feature flag:

```toml
[dependencies]
# Existing dependencies...

# Optional sink dependencies
reqwest = { version = "0.11", features = ["json"], optional = true }

[features]
# Sink feature flags
sink-http = ["dep:reqwest"]
```

## Step 2: Create the Sink

Create a new file `src/sink/http.rs`:

```rust
use etl::error::EtlResult;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

#[derive(Clone, Debug, Deserialize)]
pub struct HttpSinkConfig {
    pub url: String,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

#[derive(Clone)]
pub struct HttpSink {
    config: HttpSinkConfig,
    client: Arc<Client>,
}

impl HttpSink {
    pub fn new(config: HttpSinkConfig) -> Self {
        let client = Client::new();
        Self {
            config,
            client: Arc::new(client),
        }
    }
}

impl Sink for HttpSink {
    fn name() -> &'static str {
        "http"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        for event in events {
            let mut request = self.client
                .post(&self.config.url)
                .json(&event.payload);

            for (key, value) in &self.config.headers {
                request = request.header(key, value);
            }

            let response = request.send().await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::DestinationError,
                    "HTTP request failed",
                    e.to_string()
                )
            })?;

            if !response.status().is_success() {
                return Err(etl::etl_error!(
                    etl::error::ErrorKind::DestinationError,
                    "HTTP request failed",
                    format!("status: {}", response.status())
                ));
            }
        }

        Ok(())
    }
}
```

## Step 3: Register the Module

Update `src/sink/mod.rs`:

```rust
mod base;
pub mod memory;

#[cfg(feature = "sink-http")]
pub mod http;

pub use base::Sink;
```

## Step 4: Add to Config Enum

Update `src/config/sink.rs`:

```rust
use serde::Deserialize;

#[cfg(feature = "sink-http")]
use crate::sink::http::HttpSinkConfig;

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    Memory,

    #[cfg(feature = "sink-http")]
    Http(HttpSinkConfig),
}
```

## Step 5: Wire Up in Core

Update `src/core.rs`:

```rust
#[cfg(feature = "sink-http")]
use crate::sink::http::HttpSink;

// In start_pipeline_with_config():
let sink = match &config.sink {
    SinkConfig::Memory => MemorySink::new(),

    #[cfg(feature = "sink-http")]
    SinkConfig::Http(cfg) => HttpSink::new(cfg.clone()),
};
```

## Step 6: Build and Use

Build with your sink feature:

```bash
cargo build --release --features sink-http
```

Use in `config.yaml`:

```yaml
sink:
  type: http
  url: https://api.example.com/events
  headers:
    Authorization: Bearer token123
```

## Dynamic Routing

To support per-event routing, read from event metadata:

```rust
fn resolve_url<'a>(&'a self, event: &'a TriggeredEvent) -> &'a str {
    if let Some(ref metadata) = event.metadata {
        if let Some(url) = metadata.get("url").and_then(|v| v.as_str()) {
            return url;
        }
    }
    &self.config.url
}
```

## Error Handling

Use the ETL error types for consistent error reporting:

```rust
use etl::error::{EtlError, ErrorKind};

// Configuration errors
etl::etl_error!(ErrorKind::ConfigError, "No URL configured")

// Destination errors
etl::etl_error!(ErrorKind::DestinationError, "Failed to send", details)

// Invalid data errors
etl::etl_error!(ErrorKind::InvalidData, "Failed to serialize", details)
```

## Best Practices

1. **Use feature flags** - Don't add dependencies to the default build
2. **Support batching** - Process multiple events efficiently
3. **Handle errors gracefully** - Return appropriate error types
4. **Support dynamic routing** - Read from event metadata when possible
5. **Document configuration** - Add doc comments to config structs
