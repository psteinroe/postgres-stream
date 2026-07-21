use crate::error::{PgStreamError, PgStreamResult};
use postgres_stream::config::{PipelineConfig, load_config};
use postgres_stream::core::start_pipeline_with_config;
use postgres_stream::metrics::init_metrics;
use std::process::ExitCode;
use tracing_subscriber::{EnvFilter, fmt};

mod error;

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
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", error.render_report());
            ExitCode::FAILURE
        }
    }
}

/// Runs the daemon and propagates typed startup errors.
fn try_main() -> PgStreamResult<()> {
    let config = load_pipeline_config()?;

    init_tracing();
    init_metrics().map_err(PgStreamError::config)?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(config))?;

    Ok(())
}

fn load_pipeline_config() -> PgStreamResult<PipelineConfig> {
    load_config::<PipelineConfig>().map_err(PgStreamError::config)
}

/// Main async entry point that starts the pipeline.
///
/// Launches the stream with the provided configuration and captures
/// any errors for logging and error handling.
async fn async_main(config: PipelineConfig) -> PgStreamResult<()> {
    // Start the jemalloc metrics collection background task.
    #[cfg(not(target_env = "msvc"))]
    postgres_stream::metrics::spawn_jemalloc_metrics_task(config.stream.id);

    start_pipeline_with_config(config).await?;

    Ok(())
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

        let report = with_vars(
            [
                ("APP_CONFIG_DIR", temp_dir.path().to_str()),
                ("APP_ENVIRONMENT", Some("prod")),
            ],
            || load_pipeline_config().unwrap_err().render_report(),
        );

        assert!(report.contains("category: configuration error"));
        assert!(report.contains("failed to initialize configuration builder"));
        assert!(report.contains("cause 2:"));
        assert!(report.contains("line 2 column 1"));
    }
}
