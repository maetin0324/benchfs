//! Custom logging formatter with hostname prefix and no ANSI colors
//!
//! This module provides a custom tracing formatter that:
//! - Adds hostname prefix to each log line
//! - Removes ANSI color codes for file output
//! - Shows span hierarchy with tab indentation
//! - Includes file location and line numbers

use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Custom event formatter with hostname prefix
pub struct HostnameFormatter {
    hostname: String,
}

impl HostnameFormatter {
    pub fn new() -> Self {
        let hostname = gethostname::gethostname()
            .to_str()
            .unwrap_or("unknown")
            .to_string();
        Self { hostname }
    }
}

impl Default for HostnameFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, N> FormatEvent<S, N> for HostnameFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();

        // Get current timestamp
        let now = std::time::SystemTime::now();
        let datetime: chrono::DateTime<chrono::Utc> = now.into();

        // Write hostname prefix
        write!(writer, "[{}] ", self.hostname)?;

        // Write timestamp (no ANSI codes)
        write!(writer, "{} ", datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ"))?;

        // Write level (no ANSI codes)
        write!(writer, "{:5} ", meta.level())?;

        // Write span context with newlines and tab indentation showing hierarchy
        if let Some(scope) = ctx.event_scope() {
            // Collect all spans first
            let spans: Vec<_> = scope.from_root().collect();

            // Write each span on a new line with increasing indentation
            for (depth, span) in spans.iter().enumerate() {
                writeln!(writer)?; // New line for each span

                // Write indentation based on depth
                for _ in 0..=depth {
                    write!(writer, "\t")?;
                }

                write!(writer, "{}", span.name())?;

                // Format span fields
                let ext = span.extensions();
                if let Some(fields) = ext.get::<tracing_subscriber::fmt::FormattedFields<N>>() {
                    if !fields.is_empty() {
                        write!(writer, "{{{}}}", fields)?;
                    }
                }
            }

            // New line before the message
            writeln!(writer)?;

            // Indent the message line to match the deepest span level
            for _ in 0..=spans.len() {
                write!(writer, "\t")?;
            }
        }

        // Write target (module path)
        write!(writer, "{}", meta.target())?;

        // Write file and line
        if let Some(file) = meta.file() {
            if let Some(line) = meta.line() {
                write!(writer, ":{}:{}", file, line)?;
            }
        }

        // Write the actual message
        write!(writer, ": ")?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

/// Initialize tracing with custom hostname formatter
pub fn init_with_hostname(level: &str) {
    use tracing_subscriber::fmt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    let fmt_layer = fmt::layer()
        .event_format(HostnameFormatter::new())
        .with_writer(std::io::stderr);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();

    let hostname_os = gethostname::gethostname();
    let hostname = hostname_os.to_str().unwrap_or("unknown");
    tracing::info!("Logging initialized on host: {}", hostname);
}

/// Dump the current async task backtrace tree to stderr
pub fn dump_async_tasks() {
    eprintln!("\n========== Async Task Backtrace Dump ==========");
    eprintln!("{}", async_backtrace::taskdump_tree(true));
    eprintln!("===============================================\n");
}
