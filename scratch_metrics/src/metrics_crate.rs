//! Integration with the Rust `metrics` crate facade.
//!
//! This shows what it looks like to use the `metrics` crate as the backend
//! for SlateDB's MetricsRecorder trait. The `metrics` crate uses a global
//! recorder pattern — you install a Recorder implementation once, and then
//! macros like `counter!()` / `gauge!()` / `histogram!()` route through it.
//!
//! There are two directions to consider:
//!
//! 1. **SlateDB -> metrics crate**: Implement SlateDB's `MetricsRecorder` trait
//!    by forwarding to `metrics` crate macros/functions. This lets SlateDB emit
//!    metrics into whatever backend the user has installed globally.
//!
//! 2. **metrics crate -> SlateDB**: If SlateDB used `metrics` crate macros
//!    directly instead of its own trait, it wouldn't need MetricsRecorder at all.
//!    This is the "just adopt the metrics crate" alternative from the RFC.
//!
//! We implement approach (1) here, since the RFC keeps its own trait.

use crate::recorder::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder};
use std::sync::Arc;

// ============================================================================
// Approach 1: MetricsRecorder impl that forwards to the `metrics` crate
// ============================================================================

/// A SlateDB MetricsRecorder that forwards all metric operations to the
/// globally-installed `metrics` crate recorder.
///
/// Usage:
/// ```ignore
/// // User installs their preferred metrics backend globally
/// // (e.g., metrics-exporter-prometheus, metrics-exporter-tcp, etc.)
/// let builder = PrometheusBuilder::new();
/// builder.install().expect("failed to install recorder");
///
/// // Then pass our adapter to SlateDB
/// let db = Db::builder("my_db", object_store)
///     .with_metrics_recorder(Arc::new(MetricsCrateRecorder))
///     .open()
///     .await?;
/// ```
pub struct MetricsCrateRecorder;

impl MetricsRecorder for MetricsCrateRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        // Describe the metric (idempotent, best-effort)
        metrics::describe_counter!(name.to_string(), description.to_string());

        // Build label pairs for the metrics crate
        let label_pairs: Vec<metrics::Label> = labels
            .iter()
            .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
            .collect();

        // Get a counter handle from the global recorder
        let counter = metrics::counter!(name.to_string(), label_pairs);
        Arc::new(MetricsCrateCounter(counter))
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        metrics::describe_gauge!(name.to_string(), description.to_string());

        let label_pairs: Vec<metrics::Label> = labels
            .iter()
            .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
            .collect();

        let gauge = metrics::gauge!(name.to_string(), label_pairs);
        Arc::new(MetricsCrateGauge(gauge))
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        metrics::describe_histogram!(name.to_string(), description.to_string());

        let label_pairs: Vec<metrics::Label> = labels
            .iter()
            .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
            .collect();

        let histogram = metrics::histogram!(name.to_string(), label_pairs);
        Arc::new(MetricsCrateHistogram(histogram))
    }
}

// ============================================================================
// Adapters: wrap `metrics` crate handles to implement SlateDB traits
// ============================================================================

struct MetricsCrateCounter(metrics::Counter);

impl CounterFn for MetricsCrateCounter {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct MetricsCrateGauge(metrics::Gauge);

impl GaugeFn for MetricsCrateGauge {
    fn set(&self, value: f64) {
        self.0.set(value);
    }
    fn increment(&self, delta: f64) {
        if delta >= 0.0 {
            self.0.increment(delta);
        } else {
            self.0.decrement(-delta);
        }
    }
}

struct MetricsCrateHistogram(metrics::Histogram);

impl HistogramFn for MetricsCrateHistogram {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}

// ============================================================================
// Approach 2 (sketch): What if SlateDB used `metrics` macros directly?
// ============================================================================
//
// If SlateDB adopted the `metrics` crate directly, internal code would look
// like:
//
//   // In db.rs hot path:
//   metrics::counter!("slatedb.db.request_count", "op" => "get").increment(1);
//
//   // Or with cached handles:
//   struct DbStats {
//       get_requests: metrics::Counter,
//   }
//   impl DbStats {
//       fn new() -> Self {
//           Self {
//               get_requests: metrics::counter!("slatedb.db.request_count", "op" => "get"),
//           }
//       }
//   }
//
// Pros:
//   - No custom trait needed
//   - Entire `metrics` ecosystem works automatically (prometheus, statsd, etc.)
//   - Well-tested, widely used
//
// Cons (reasons the RFC rejected this):
//   - Global state: `set_global_recorder()` is process-wide. Multiple Db
//     instances can't have different recorders. This matters for testing
//     and multi-tenant use cases.
//   - External dependency: users might already have a `metrics` recorder
//     installed, and only one global recorder is allowed.
//   - UniFFI: the trait definitions need to be owned by SlateDB for FFI.
//   - db.metrics(): still need an always-on default, so you'd end up with
//     a composite pattern anyway (or use metrics-util's DebuggingRecorder).
//
// The RFC's approach is essentially "own the trait, but make it trivial to
// bridge to the `metrics` crate" — which is what MetricsCrateRecorder above
// demonstrates in ~100 lines.

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorder::{CompositeMetricsRecorder, DefaultMetricsRecorder};
    use std::sync::atomic::Ordering;

    #[test]
    fn should_create_handles_via_metrics_crate() {
        // Use a local recorder scope so we don't pollute global state.
        // This demonstrates the global-state problem: in tests, you can't
        // easily install different recorders per test.
        let recorder = MetricsCrateRecorder;

        // These will go to the noop recorder (no global installed), but
        // the point is that the adapter compiles and the types work.
        let counter = recorder.register_counter(
            "slatedb.db.request_count",
            "Number of DB requests",
            &[("op", "get")],
        );
        let gauge = recorder.register_gauge(
            "slatedb.db.wal_buffer_estimated_bytes",
            "WAL buffer size",
            &[],
        );
        let histogram = recorder.register_histogram(
            "slatedb.db.request_duration_seconds",
            "Request latency",
            &[("op", "get")],
        );

        // These calls succeed (go to noop since no global recorder installed)
        counter.increment(1);
        gauge.set(1024.0);
        gauge.increment(-512.0);
        histogram.record(0.005);
    }

    #[test]
    fn should_work_as_composite_user_recorder() {
        // The real value: MetricsCrateRecorder plugs into the composite
        // recorder just like OTel or Prometheus recorders do.
        let composite = CompositeMetricsRecorder {
            default: DefaultMetricsRecorder::new(),
            user: Some(Arc::new(MetricsCrateRecorder)),
        };

        let counter = composite.register_counter(
            "slatedb.db.request_count",
            "Number of DB requests",
            &[("op", "get")],
        );

        counter.increment(3);

        // Default recorder still tracks it for db.metrics()
        let entries = composite.default.entries.lock().unwrap();
        let entry = entries.iter().find(|e| e.name == "slatedb.db.request_count").unwrap();
        match &entry.kind {
            crate::recorder::DefaultMetricKind::Counter(c) => {
                assert_eq!(c.0.load(Ordering::Relaxed), 3);
            }
            _ => panic!("expected counter"),
        }

        // AND the metrics crate got the event too (goes to noop here,
        // but in prod it would go to whatever global recorder is installed)
    }
}
