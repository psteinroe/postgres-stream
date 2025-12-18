use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

use super::{register_failover_metrics, register_maintenance_metrics, register_stream_metrics};

/// Initializes metrics with an automatic HTTP server on port 9000.
///
/// This function is designed for standalone services where metrics should be exposed
/// automatically without manual endpoint management. It installs a global metrics recorder
/// and starts an HTTP server that listens on `[::]:9000/metrics`, making metrics available
/// for Prometheus scraping.
///
/// # Use Case
///
/// Use this when you want to:
/// - Expose metrics from a standalone service.
/// - Automatically start a dedicated metrics endpoint without custom routing.
/// - Let Prometheus scrape metrics directly from a fixed port.
pub fn init_metrics() -> Result<(), BuildError> {
    let builder = PrometheusBuilder::new().with_http_listener(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        9000,
    ));

    builder.install()?;

    // Register all metric descriptions
    register_failover_metrics();
    register_maintenance_metrics();
    register_stream_metrics();

    Ok(())
}
