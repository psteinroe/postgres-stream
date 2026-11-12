use std::sync::Once;

/// Ensures the crypto provider is only installed once.
static INIT_CRYPTO: Once = Once::new();

/// Installs the process-level cryptographic provider for rustls.
///
/// Both the `aws-lc-rs` and `ring` rustls backends are compiled in (etl uses the former, sqlx the
/// latter), so rustls cannot pick one on its own and building a TLS client config without a
/// process-level provider panics. Must be called before any TLS connection is opened. Keeps an
/// already installed provider.
pub fn init_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
