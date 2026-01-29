//! Global statistics collection control
//!
//! This module provides a global flag to enable/disable detailed timing
//! statistics collection. When disabled, timing measurements are skipped
//! to avoid performance overhead during production benchmarks.

use std::sync::atomic::{AtomicBool, Ordering};

/// Global flag for enabling detailed statistics collection
static STATS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable or disable statistics collection globally
pub fn set_stats_enabled(enabled: bool) {
    STATS_ENABLED.store(enabled, Ordering::SeqCst);
    // Also set the pluvio_ucx stats flag
    pluvio_ucx::stats::set_stats_enabled(enabled);
}

/// Check if statistics collection is enabled
#[inline]
pub fn is_stats_enabled() -> bool {
    STATS_ENABLED.load(Ordering::Relaxed)
}

/// Macro to conditionally execute timing code only when stats are enabled
#[macro_export]
macro_rules! if_stats {
    ($($code:tt)*) => {
        if $crate::stats::is_stats_enabled() {
            $($code)*
        }
    };
}

/// Macro to measure elapsed time only when stats are enabled
/// Returns Option<Duration> - Some(duration) if stats enabled, None otherwise
#[macro_export]
macro_rules! measure_if_stats {
    ($start:expr) => {
        if $crate::stats::is_stats_enabled() {
            Some($start.elapsed())
        } else {
            None
        }
    };
}
