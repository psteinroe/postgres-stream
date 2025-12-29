//! Global test lock for coordinating tests that modify Postgres system settings.
//!
//! Tests that use `ALTER SYSTEM` commands affect the entire Postgres server,
//! which can interfere with parallel tests. This module provides a global RwLock
//! that ensures:
//! - Tests modifying system settings acquire exclusive (write) access
//! - Regular tests acquire shared (read) access
//! - System-modifying tests run in isolation while regular tests can run concurrently

use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Global lock for coordinating test access to Postgres system settings.
///
/// This is a RwLock where:
/// - Write lock: exclusive access for tests that modify system settings
/// - Read lock: shared access for regular tests
static SYSTEM_SETTINGS_LOCK: OnceLock<Arc<RwLock<()>>> = OnceLock::new();

fn get_lock() -> Arc<RwLock<()>> {
    SYSTEM_SETTINGS_LOCK
        .get_or_init(|| Arc::new(RwLock::new(())))
        .clone()
}

/// Acquire shared access for tests that don't modify system settings.
///
/// Multiple tests can hold this lock concurrently, but they will wait
/// if a system-modifying test has exclusive access.
pub async fn acquire_shared_test_lock() -> OwnedRwLockReadGuard<()> {
    get_lock().read_owned().await
}

/// Acquire exclusive access for tests that modify system settings.
///
/// This waits for all other tests to finish and prevents any new tests
/// from starting until the system-modifying test completes.
pub async fn acquire_exclusive_test_lock() -> OwnedRwLockWriteGuard<()> {
    get_lock().write_owned().await
}
