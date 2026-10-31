/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

//! The in-memory stats in the Prometheus text format, version 0.0.4.
//!
//! Every stat is one family, named after the stat with each character
//! outside `[A-Za-z0-9_]` replaced by `_`. Counters and timeseries are
//! summaries without quantiles, with their sum and count since the stat was
//! created. Histograms and quantile stats are summaries with the 0.5, 0.95
//! and 0.99 quantiles of the last minute, and the same sum and count.
//! Singleton counters are gauges. A family whose names were taken by an
//! earlier one is left out and counted.

use std::collections::HashSet;
use std::fmt;
use std::fmt::Write;

use stats::memory_stats::Snapshot;
use stats::memory_stats::StatKind;
use stats::memory_stats::StatSnapshot;

/// The content type of [`render_prometheus`]'s output.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

const UPTIME: &str = "mononoke_process_uptime_seconds";
const DROPPED: &str = "mononoke_stats_dropped";
const SKIPPED: &str = "mononoke_stats_export_skipped";

const QUANTILES: [(f64, &str); 3] = [(0.5, "0.5"), (0.95, "0.95"), (0.99, "0.99")];

/// Renders every stat of `snapshot`, then the uptime, the stats the backend
/// dropped, and the families left out here.
pub fn render_prometheus(snapshot: &Snapshot) -> String {
    let mut out = String::new();
    render(snapshot, &mut out).expect("writing to a String cannot fail");
    out
}

fn render(snapshot: &Snapshot, out: &mut String) -> fmt::Result {
    // Names already used, by a family or by its samples.
    let mut taken: HashSet<String> = [UPTIME, DROPPED, SKIPPED]
        .into_iter()
        .map(String::from)
        .collect();
    let mut skipped = 0u64;
    for stat in &snapshot.stats {
        let name = metric_name(&stat.name);
        let names = family_names(&name, stat.kind);
        if names.iter().any(|name| taken.contains(name)) {
            skipped += 1;
            continue;
        }
        taken.extend(names);
        render_stat(stat, &name, out)?;
    }
    let uptime = "Time since the in-memory stats backend was installed, in seconds.";
    render_gauge(out, UPTIME, uptime, snapshot.uptime.as_secs())?;
    let dropped = "Stats the in-memory backend does not keep: over its limits, or a name reused with another kind.";
    render_gauge(out, DROPPED, dropped, snapshot.dropped)?;
    let skipped_help = "Stats left out of this export because their names were taken.";
    render_gauge(out, SKIPPED, skipped_help, skipped)
}

/// `name` as a metric name: every character outside `[A-Za-z0-9_]` becomes
/// `_`, and a leading digit gets a `_` before it.
fn metric_name(name: &str) -> String {
    let mut metric: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    if metric.is_empty() || metric.starts_with(|c: char| c.is_ascii_digit()) {
        metric.insert(0, '_');
    }
    metric
}

/// The names a family of `kind` named `name` uses.
fn family_names(name: &str, kind: StatKind) -> Vec<String> {
    match kind {
        StatKind::Singleton => vec![name.to_string()],
        StatKind::Counter | StatKind::Timeseries | StatKind::Histogram | StatKind::Quantile => {
            vec![
                name.to_string(),
                format!("{name}_sum"),
                format!("{name}_count"),
            ]
        }
    }
}

fn render_stat(stat: &StatSnapshot, name: &str, out: &mut String) -> fmt::Result {
    let kind = match stat.kind {
        StatKind::Singleton => "gauge",
        StatKind::Counter | StatKind::Timeseries | StatKind::Histogram | StatKind::Quantile => {
            "summary"
        }
    };
    writeln!(out, "# HELP {name} {}", escape_help(&stat.name))?;
    writeln!(out, "# TYPE {name} {kind}")?;
    match stat.kind {
        StatKind::Singleton => writeln!(out, "{name} {}", stat.total.sum),
        StatKind::Counter | StatKind::Timeseries => {
            writeln!(out, "{name}_sum {}", stat.total.sum)?;
            writeln!(out, "{name}_count {}", stat.total.count)
        }
        StatKind::Histogram | StatKind::Quantile => {
            for (q, label) in QUANTILES {
                match stat.recent.quantile(q) {
                    Some(value) => writeln!(out, "{name}{{quantile=\"{label}\"}} {value}")?,
                    None => writeln!(out, "{name}{{quantile=\"{label}\"}} NaN")?,
                }
            }
            writeln!(out, "{name}_sum {}", stat.total.sum)?;
            writeln!(out, "{name}_count {}", stat.total.count)
        }
    }
}

fn render_gauge(out: &mut String, name: &str, help: &str, value: u64) -> fmt::Result {
    writeln!(out, "# HELP {name} {help}")?;
    writeln!(out, "# TYPE {name} gauge")?;
    writeln!(out, "{name} {value}")
}

/// `text` as a HELP line may hold it.
fn escape_help(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mononoke_macros::mononoke;
    use stats::memory_stats::Distribution;
    use stats::memory_stats::Window;

    use super::*;

    fn stat(name: &str, kind: StatKind, total: Window, recent: Window) -> StatSnapshot {
        StatSnapshot {
            name: name.to_string(),
            kind,
            total,
            recent,
        }
    }

    fn window(sum: i64, count: u64) -> Window {
        Window {
            sum,
            count,
            distribution: None,
        }
    }

    fn distribution(values: &[i64]) -> Window {
        let mut distribution = Distribution::new();
        for value in values {
            distribution.record(*value, 1);
        }
        Window {
            sum: values.iter().sum(),
            count: values.len() as u64,
            distribution: Some(distribution),
        }
    }

    fn snapshot(stats: Vec<StatSnapshot>) -> Snapshot {
        Snapshot {
            uptime: Duration::from_millis(5_234_900),
            window: Duration::from_secs(57),
            dropped: 2,
            stats,
        }
    }

    #[mononoke::test]
    fn test_every_kind() {
        let text = render_prometheus(&snapshot(vec![
            stat("a.counter", StatKind::Counter, window(5, 3), window(1, 1)),
            stat(
                "a.histogram",
                StatKind::Histogram,
                distribution(&[1, 2, 3, 7, 100]),
                distribution(&[1, 2, 7]),
            ),
            stat(
                "a.quantile",
                StatKind::Quantile,
                distribution(&[4]),
                distribution(&[]),
            ),
            stat(
                "a.singleton",
                StatKind::Singleton,
                window(-42, 1),
                window(-42, 1),
            ),
            stat(
                "sql.mysql_ffi.my-label.get_connection_ms",
                StatKind::Timeseries,
                window(12, 4),
                window(0, 0),
            ),
        ]));
        let expected = "\
# HELP a_counter a.counter
# TYPE a_counter summary
a_counter_sum 5
a_counter_count 3
# HELP a_histogram a.histogram
# TYPE a_histogram summary
a_histogram{quantile=\"0.5\"} 2
a_histogram{quantile=\"0.95\"} 7
a_histogram{quantile=\"0.99\"} 7
a_histogram_sum 113
a_histogram_count 5
# HELP a_quantile a.quantile
# TYPE a_quantile summary
a_quantile{quantile=\"0.5\"} NaN
a_quantile{quantile=\"0.95\"} NaN
a_quantile{quantile=\"0.99\"} NaN
a_quantile_sum 4
a_quantile_count 1
# HELP a_singleton a.singleton
# TYPE a_singleton gauge
a_singleton -42
# HELP sql_mysql_ffi_my_label_get_connection_ms sql.mysql_ffi.my-label.get_connection_ms
# TYPE sql_mysql_ffi_my_label_get_connection_ms summary
sql_mysql_ffi_my_label_get_connection_ms_sum 12
sql_mysql_ffi_my_label_get_connection_ms_count 4
# HELP mononoke_process_uptime_seconds Time since the in-memory stats backend was installed, in seconds.
# TYPE mononoke_process_uptime_seconds gauge
mononoke_process_uptime_seconds 5234
# HELP mononoke_stats_dropped Stats the in-memory backend does not keep: over its limits, or a name reused with another kind.
# TYPE mononoke_stats_dropped gauge
mononoke_stats_dropped 2
# HELP mononoke_stats_export_skipped Stats left out of this export because their names were taken.
# TYPE mononoke_stats_export_skipped gauge
mononoke_stats_export_skipped 0
";
        assert_eq!(text, expected);
    }

    #[mononoke::test]
    fn test_names() {
        assert_eq!(metric_name("9lives"), "_9lives");
        assert_eq!(metric_name("a.b-c:d/e"), "a_b_c_d_e");
        assert_eq!(metric_name("caf\u{e9}"), "caf_");
        assert_eq!(metric_name(""), "_");
        assert_eq!(escape_help("a\\b\nc"), "a\\\\b\\nc");

        let text = render_prometheus(&snapshot(vec![stat(
            "9lives\\with\nnewline",
            StatKind::Singleton,
            window(1, 1),
            window(1, 1),
        )]));
        assert!(
            text.starts_with(
                "# HELP _9lives_with_newline 9lives\\\\with\\nnewline\n\
                 # TYPE _9lives_with_newline gauge\n\
                 _9lives_with_newline 1\n"
            ),
            "{text}"
        );
    }

    #[mononoke::test]
    fn test_collisions_are_skipped() {
        let counter = |name: &str| stat(name, StatKind::Counter, window(1, 1), window(1, 1));
        let gauge = |name: &str| stat(name, StatKind::Singleton, window(1, 1), window(1, 1));
        let text = render_prometheus(&snapshot(vec![
            counter("a.b"),
            // The same metric name.
            counter("a_b"),
            // A sample name of a.b.
            gauge("a.b_count"),
            // A family whose samples would take the name of a.b's.
            counter("a.b.sum"),
            counter("a.b_sum.x"),
            // One of the names added at the end.
            gauge("mononoke.stats.dropped"),
        ]));
        assert!(text.contains("# HELP a_b a.b\n"), "{text}");
        assert!(!text.contains("# HELP a_b a_b\n"), "{text}");
        assert!(!text.contains("a.b_count"), "{text}");
        assert!(!text.contains("# HELP a_b_sum "), "{text}");
        assert!(text.contains("# HELP a_b_sum_x a.b_sum.x\n"), "{text}");
        assert!(!text.contains("mononoke.stats.dropped"), "{text}");
        assert!(
            text.ends_with("mononoke_stats_export_skipped 4\n"),
            "{text}"
        );
        assert_eq!(text.matches("# TYPE mononoke_stats_dropped").count(), 1);
    }
}
