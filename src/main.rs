use anyhow::Context;
use etl::error::EtlResult;
use postgres_stream::config::{PipelineConfig, load_config};
use postgres_stream::core::start_pipeline_with_config;
use postgres_stream::metrics::init_metrics;
use std::{error::Error as StdError, fmt as stdfmt, process::ExitCode};
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt};

/// Jemalloc allocator for better memory management in high-throughput async workloads.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Jemalloc configuration optimized for high-throughput async CDC workloads.
///
/// - `narenas:8`: Fixed arena count for predictable memory behavior in containers.
/// - `background_thread:true`: Offloads memory purging to background threads (Linux only).
/// - `metadata_thp:auto`: Enables transparent huge pages for jemalloc metadata, reducing TLB misses.
/// - `dirty_decay_ms:10000`: Returns unused dirty pages to the OS after 10 seconds.
/// - `muzzy_decay_ms:10000`: Returns unused muzzy pages to the OS after 10 seconds.
/// - `tcache_max:8192`: Reduces thread-local cache size for better container memory efficiency.
/// - `abort_conf:true`: Aborts on invalid configuration for fail-fast behavior.
///
/// On Linux, this can be overridden via `MALLOC_CONF` env var.
/// On macOS, use `_RJEM_MALLOC_CONF` (unprefixed symbols not supported).
#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

/// Jemalloc configuration for macOS (uses prefixed symbol since unprefixed not supported).
#[cfg(all(target_os = "macos", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

/// Entry point for the pgstream daemon.
///
/// Loads configuration, initializes tracing, starts the async runtime,
/// and launches the replication stream. Handles all errors and ensures
/// proper service initialization sequence.
fn main() -> ExitCode {
    init_tracing();

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!("an error occurred in the stream daemon: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// Loads configuration and runs the daemon.
fn run() -> anyhow::Result<()> {
    let config = load_pipeline_config()?;

    init_metrics()?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(config))?;

    Ok(())
}

/// Loads pipeline configuration while retaining the complete source chain.
fn load_pipeline_config() -> anyhow::Result<PipelineConfig> {
    load_config::<PipelineConfig>()
        .map_err(|error| SafeErrorChain::from_error(&error))
        .context("failed to load configuration")
}

/// A source-preserving error chain with configuration values redacted from each message.
#[derive(Debug)]
struct SafeErrorChain {
    message: String,
    source: Option<Box<Self>>,
}

impl SafeErrorChain {
    fn from_error(error: &(dyn StdError + 'static)) -> Self {
        Self {
            message: redact_configuration_values(&error.to_string()),
            source: error
                .source()
                .map(|source| Box::new(Self::from_error(source))),
        }
    }
}

impl stdfmt::Display for SafeErrorChain {
    fn fmt(&self, formatter: &mut stdfmt::Formatter<'_>) -> stdfmt::Result {
        formatter.write_str(&self.message)
    }
}

impl StdError for SafeErrorChain {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

fn redact_configuration_values(message: &str) -> String {
    let mut redacted = message.to_string();

    for prefix in ["invalid type: ", "invalid value: ", "unknown variant "] {
        redact_between(&mut redacted, prefix, ", expected");
    }

    redact_urls(&mut redacted);
    redacted
}

fn redact_between(message: &mut String, prefix: &str, suffix: &str) {
    let Some(start) = message.find(prefix).map(|index| index + prefix.len()) else {
        return;
    };
    let Some(end) = message[start..].find(suffix).map(|index| start + index) else {
        return;
    };

    message.replace_range(start..end, "<redacted>");
}

fn redact_urls(message: &mut String) {
    const DELIMITERS: &[char] = &[
        ' ', '\t', '\n', '\r', '"', '\'', '`', '<', '>', '[', ']', '(', ')',
    ];

    while let Some(scheme_end) = message.find("://") {
        let start = message[..scheme_end]
            .char_indices()
            .rev()
            .find(|(_, character)| DELIMITERS.contains(character))
            .map_or(0, |(index, character)| index + character.len_utf8());
        let value_end = scheme_end + 3;
        let end = message[value_end..]
            .char_indices()
            .find(|(_, character)| DELIMITERS.contains(character))
            .map_or(message.len(), |(index, _)| value_end + index);

        message.replace_range(start..end, "<redacted-url>");
    }
}

/// Main async entry point that starts the pipeline.
///
/// Launches the stream with the provided configuration and captures
/// any errors for logging and error handling.
async fn async_main(config: PipelineConfig) -> EtlResult<()> {
    // Start the jemalloc metrics collection background task.
    #[cfg(not(target_env = "msvc"))]
    postgres_stream::metrics::spawn_jemalloc_metrics_task(config.stream.id);

    start_pipeline_with_config(config).await
}

/// Initializes the tracing subscriber for logging.
///
/// Sets up a basic console-based logger with INFO level by default.
/// Can be configured via RUST_LOG environment variable.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use temp_env::with_vars;
    use tempfile::TempDir;

    #[test]
    fn malformed_configuration_renders_the_underlying_cause() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join("base.yml"), "stream: [\n").unwrap();

        let rendered = with_vars(
            [
                ("APP_CONFIG_DIR", temp_dir.path().to_str()),
                ("APP_ENVIRONMENT", Some("prod")),
            ],
            || {
                let error = load_pipeline_config().unwrap_err();
                format!("{error:#}")
            },
        );

        assert!(rendered.contains("failed to load configuration"));
        assert!(rendered.contains("failed to initialize configuration builder"));
        assert!(rendered.contains("line 2 column 1"));
    }

    #[test]
    fn configuration_errors_do_not_render_values_or_secrets() {
        let temp_dir = TempDir::new().unwrap();
        let base = r#"
stream:
  id: 1
  pg_connection:
    host: localhost
    port: 5432
    name: postgres
    username: postgres
    password: null
    tls:
      enabled: false
      trusted_root_certs: ""
  batch:
    max_size: 100
    max_fill_ms: 50
sink:
  type: "redis://user:DO_NOT_LOG_THIS_SECRET@localhost:6379"
"#;
        fs::write(temp_dir.path().join("base.yml"), base).unwrap();

        let rendered = with_vars(
            [
                ("APP_CONFIG_DIR", temp_dir.path().to_str()),
                ("APP_ENVIRONMENT", Some("prod")),
            ],
            || {
                let error = load_pipeline_config().unwrap_err();
                format!("{error:#}")
            },
        );

        assert!(rendered.contains("failed to deserialize configuration"));
        assert!(rendered.contains("unknown variant <redacted>"));
        assert!(!rendered.contains("DO_NOT_LOG_THIS_SECRET"));
        assert!(!rendered.contains("redis://"));
    }
}
