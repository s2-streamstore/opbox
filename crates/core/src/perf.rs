/// Start a performance timer (no-op under the `sim` feature).
#[macro_export]
macro_rules! perf_start {
    () => {{
        #[cfg(not(feature = "sim"))]
        {
            ::std::time::Instant::now()
        }
        #[cfg(feature = "sim")]
        {
            ()
        }
    }};
}

/// Emit a `trace!` event with elapsed microseconds (no-op elapsed under `sim`).
#[macro_export]
macro_rules! trace_perf {
    ($started:expr, $($fields:tt)*) => {{
        #[cfg(not(feature = "sim"))]
        {
            let elapsed_us = $started.elapsed().as_micros() as u64;
            ::tracing::trace!(elapsed_us, $($fields)*);
        }
        #[cfg(feature = "sim")]
        {
            let _ = &$started;
            ::tracing::trace!($($fields)*);
        }
    }};
}
