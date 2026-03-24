//! # Metrics Module
//!
//! Provides a recorder-based metrics system that supports labels, histograms, and
//! user-pluggable backends (Prometheus, OTLP, etc.) while preserving a zero-dependency,
//! always-on default for `db.metrics()` debugging.
//!
//! ## Components
//!
//! * [`MetricsRecorder`]: User-implemented trait to bridge SlateDB metrics to their
//!   observability system. Passed via `DbBuilder`. If not provided, only the built-in
//!   default recorder is used.
//!
//! * [`CounterFn`], [`GaugeFn`], [`HistogramFn`]: Trait-based metric handles returned
//!   by the recorder. Internal components cache these handles and call them on the hot path.
//!
//! * [`DefaultMetricsRecorder`]: Atomic-backed recorder that powers `db.metrics()`.
//!
//! * [`CompositeMetricsRecorder`]: Wraps the default recorder and an optional user recorder,
//!   forwarding all operations to both.
//!
//! * [`Metrics`], [`Metric`], [`MetricValue`]: Materialized snapshot types returned by
//!   `db.metrics()`.
//!
//! [`Db::metrics`]: crate::db::Db::metrics

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Core traits
// ---------------------------------------------------------------------------

/// User-implemented trait to bridge SlateDB metrics to their observability system.
/// Passed via DbBuilder. If not provided, only the built-in default recorder is used.
pub trait MetricsRecorder: Send + Sync {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn>;

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn>;

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn>;
}

pub trait CounterFn: Send + Sync {
    fn increment(&self, value: u64);
}

pub trait GaugeFn: Send + Sync {
    fn set(&self, value: f64);
    /// Increment (or decrement with a negative value) the gauge.
    fn increment(&self, value: f64);
}

pub trait HistogramFn: Send + Sync {
    fn record(&self, value: f64);
}

// ---------------------------------------------------------------------------
// Snapshot types (returned by db.metrics())
// ---------------------------------------------------------------------------

/// A single metric's current value along with its metadata.
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub description: String,
    pub value: MetricValue,
}

/// The value of a metric at the time of the snapshot.
#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

/// Materialized snapshot of all registered metrics, with lookup methods.
#[derive(Debug, Clone)]
pub struct Metrics {
    metrics: Vec<Metric>,
}

impl Metrics {
    /// Return all metrics.
    pub fn all(&self) -> &[Metric] {
        &self.metrics
    }

    /// Look up all metrics matching a given name (any labels).
    pub fn by_name(&self, name: &str) -> Vec<&Metric> {
        self.metrics.iter().filter(|m| m.name == name).collect()
    }

    /// Look up the unique metric matching a name and exact canonical label set.
    /// Input label order does not matter.
    pub fn by_name_and_labels(&self, name: &str, labels: &[(&str, &str)]) -> Option<&Metric> {
        let mut sorted: Vec<(&str, &str)> = labels.to_vec();
        sorted.sort();
        self.metrics.iter().find(|m| {
            m.name == name && {
                let mut ml: Vec<(&str, &str)> = m
                    .labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                ml.sort();
                ml == sorted
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Default (atomic-backed) recorder
// ---------------------------------------------------------------------------

struct AtomicCounter {
    value: AtomicU64,
}

impl AtomicCounter {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    fn load(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl CounterFn for AtomicCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }
}

struct AtomicGauge {
    bits: AtomicU64,
}

impl AtomicGauge {
    fn new() -> Self {
        Self {
            bits: AtomicU64::new(0f64.to_bits()),
        }
    }

    fn load(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

impl GaugeFn for AtomicGauge {
    fn set(&self, value: f64) {
        self.bits.store(value.to_bits(), Ordering::Relaxed);
    }

    fn increment(&self, value: f64) {
        loop {
            let current = self.bits.load(Ordering::Relaxed);
            let new = f64::from_bits(current) + value;
            if self
                .bits
                .compare_exchange_weak(current, new.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

struct AtomicHistogram {
    count: AtomicU64,
    sum_bits: AtomicU64,
    min_bits: AtomicU64,
    max_bits: AtomicU64,
}

impl AtomicHistogram {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_bits: AtomicU64::new(0f64.to_bits()),
            min_bits: AtomicU64::new(f64::INFINITY.to_bits()),
            max_bits: AtomicU64::new(f64::NEG_INFINITY.to_bits()),
        }
    }

    fn load(&self) -> (u64, f64, f64, f64) {
        (
            self.count.load(Ordering::Relaxed),
            f64::from_bits(self.sum_bits.load(Ordering::Relaxed)),
            f64::from_bits(self.min_bits.load(Ordering::Relaxed)),
            f64::from_bits(self.max_bits.load(Ordering::Relaxed)),
        )
    }
}

impl HistogramFn for AtomicHistogram {
    fn record(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);

        // CAS loop for sum
        loop {
            let current = self.sum_bits.load(Ordering::Relaxed);
            let new = f64::from_bits(current) + value;
            if self
                .sum_bits
                .compare_exchange_weak(current, new.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // CAS loop for min
        loop {
            let current = self.min_bits.load(Ordering::Relaxed);
            let current_val = f64::from_bits(current);
            if value >= current_val {
                break;
            }
            if self
                .min_bits
                .compare_exchange_weak(
                    current,
                    value.to_bits(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // CAS loop for max
        loop {
            let current = self.max_bits.load(Ordering::Relaxed);
            let current_val = f64::from_bits(current);
            if value <= current_val {
                break;
            }
            if self
                .max_bits
                .compare_exchange_weak(
                    current,
                    value.to_bits(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DefaultMetricsRecorder
// ---------------------------------------------------------------------------

enum DefaultHandle {
    Counter(Arc<AtomicCounter>),
    Gauge(Arc<AtomicGauge>),
    Histogram(Arc<AtomicHistogram>),
}

struct MetricEntry {
    name: String,
    description: String,
    labels: Vec<(String, String)>,
    handle: DefaultHandle,
}

/// The built-in atomic-backed recorder that powers `db.metrics()`.
pub struct DefaultMetricsRecorder {
    entries: Mutex<Vec<MetricEntry>>,
}

impl Default for DefaultMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultMetricsRecorder {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    /// Take a snapshot of all registered metrics.
    pub fn snapshot(&self) -> Metrics {
        let entries = self.entries.lock().expect("lock poisoned");
        let metrics = entries
            .iter()
            .map(|entry| {
                let value = match &entry.handle {
                    DefaultHandle::Counter(c) => MetricValue::Counter(c.load()),
                    DefaultHandle::Gauge(g) => MetricValue::Gauge(g.load()),
                    DefaultHandle::Histogram(h) => {
                        let (count, sum, min, max) = h.load();
                        MetricValue::Histogram {
                            count,
                            sum,
                            min,
                            max,
                        }
                    }
                };
                Metric {
                    name: entry.name.clone(),
                    description: entry.description.clone(),
                    labels: entry.labels.clone(),
                    value,
                }
            })
            .collect();
        Metrics { metrics }
    }
}

fn canonicalize_labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut sorted: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    sorted.sort();
    sorted
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let handle = Arc::new(AtomicCounter::new());
        let mut entries = self.entries.lock().expect("lock poisoned");
        entries.push(MetricEntry {
            name: name.to_string(),
            description: description.to_string(),
            labels: canonicalize_labels(labels),
            handle: DefaultHandle::Counter(handle.clone()),
        });
        handle
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let handle = Arc::new(AtomicGauge::new());
        let mut entries = self.entries.lock().expect("lock poisoned");
        entries.push(MetricEntry {
            name: name.to_string(),
            description: description.to_string(),
            labels: canonicalize_labels(labels),
            handle: DefaultHandle::Gauge(handle.clone()),
        });
        handle
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        let handle = Arc::new(AtomicHistogram::new());
        let mut entries = self.entries.lock().expect("lock poisoned");
        entries.push(MetricEntry {
            name: name.to_string(),
            description: description.to_string(),
            labels: canonicalize_labels(labels),
            handle: DefaultHandle::Histogram(handle.clone()),
        });
        handle
    }
}

// ---------------------------------------------------------------------------
// Composite handles
// ---------------------------------------------------------------------------

struct CompositeCounter {
    default: Arc<dyn CounterFn>,
    user: Option<Arc<dyn CounterFn>>,
}

impl CounterFn for CompositeCounter {
    fn increment(&self, value: u64) {
        self.default.increment(value);
        if let Some(user) = &self.user {
            user.increment(value);
        }
    }
}

struct CompositeGauge {
    default: Arc<dyn GaugeFn>,
    user: Option<Arc<dyn GaugeFn>>,
}

impl GaugeFn for CompositeGauge {
    fn set(&self, value: f64) {
        self.default.set(value);
        if let Some(user) = &self.user {
            user.set(value);
        }
    }

    fn increment(&self, value: f64) {
        self.default.increment(value);
        if let Some(user) = &self.user {
            user.increment(value);
        }
    }
}

struct CompositeHistogram {
    default: Arc<dyn HistogramFn>,
    user: Option<Arc<dyn HistogramFn>>,
}

impl HistogramFn for CompositeHistogram {
    fn record(&self, value: f64) {
        self.default.record(value);
        if let Some(user) = &self.user {
            user.record(value);
        }
    }
}

// ---------------------------------------------------------------------------
// CompositeMetricsRecorder
// ---------------------------------------------------------------------------

/// Wraps the default recorder and an optional user recorder, forwarding
/// all registrations and operations to both.
pub struct CompositeMetricsRecorder {
    default: Arc<DefaultMetricsRecorder>,
    user: Option<Arc<dyn MetricsRecorder>>,
}

impl CompositeMetricsRecorder {
    pub fn new(
        default: Arc<DefaultMetricsRecorder>,
        user: Option<Arc<dyn MetricsRecorder>>,
    ) -> Self {
        Self { default, user }
    }

    /// Take a snapshot from the default recorder.
    pub fn snapshot(&self) -> Metrics {
        self.default.snapshot()
    }
}

impl MetricsRecorder for CompositeMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let default_handle = self.default.register_counter(name, description, labels);
        let user_handle = self
            .user
            .as_ref()
            .map(|u| u.register_counter(name, description, labels));
        Arc::new(CompositeCounter {
            default: default_handle,
            user: user_handle,
        })
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let default_handle = self.default.register_gauge(name, description, labels);
        let user_handle = self
            .user
            .as_ref()
            .map(|u| u.register_gauge(name, description, labels));
        Arc::new(CompositeGauge {
            default: default_handle,
            user: user_handle,
        })
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        let default_handle = self.default.register_histogram(name, description, labels);
        let user_handle = self
            .user
            .as_ref()
            .map(|u| u.register_histogram(name, description, labels));
        Arc::new(CompositeHistogram {
            default: default_handle,
            user: user_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_counter() {
        let recorder = DefaultMetricsRecorder::new();
        let counter = recorder.register_counter("test.counter", "a test counter", &[]);
        counter.increment(1);
        counter.increment(5);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.counter");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 6),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_track_gauge_set() {
        let recorder = DefaultMetricsRecorder::new();
        let gauge = recorder.register_gauge("test.gauge", "a test gauge", &[]);
        gauge.set(42.0);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.gauge");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Gauge(v) => assert!((v - 42.0).abs() < f64::EPSILON),
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn should_track_gauge_increment() {
        let recorder = DefaultMetricsRecorder::new();
        let gauge = recorder.register_gauge("test.gauge", "a test gauge", &[]);
        gauge.set(10.0);
        gauge.increment(5.0);
        gauge.increment(-3.0);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.gauge");
        match &metric[0].value {
            MetricValue::Gauge(v) => assert!((v - 12.0).abs() < f64::EPSILON),
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn should_track_histogram() {
        let recorder = DefaultMetricsRecorder::new();
        let hist = recorder.register_histogram("test.hist", "a test histogram", &[]);
        hist.record(1.0);
        hist.record(2.0);
        hist.record(3.0);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.hist");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Histogram {
                count,
                sum,
                min,
                max,
            } => {
                assert_eq!(*count, 3);
                assert!((sum - 6.0).abs() < f64::EPSILON);
                assert!((min - 1.0).abs() < f64::EPSILON);
                assert!((max - 3.0).abs() < f64::EPSILON);
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    #[test]
    fn should_track_empty_histogram() {
        let recorder = DefaultMetricsRecorder::new();
        let _hist = recorder.register_histogram("test.hist", "empty", &[]);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.hist");
        match &metric[0].value {
            MetricValue::Histogram {
                count, min, max, ..
            } => {
                assert_eq!(*count, 0);
                assert!(min.is_infinite() && min.is_sign_positive());
                assert!(max.is_infinite() && max.is_sign_negative());
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    #[test]
    fn should_support_labels() {
        let recorder = DefaultMetricsRecorder::new();
        recorder.register_counter(
            "slatedb.db.request_count",
            "Number of requests",
            &[("op", "get")],
        );
        recorder.register_counter(
            "slatedb.db.request_count",
            "Number of requests",
            &[("op", "scan")],
        );

        let snapshot = recorder.snapshot();
        let metrics = snapshot.by_name("slatedb.db.request_count");
        assert_eq!(metrics.len(), 2);

        let get_metric = snapshot.by_name_and_labels("slatedb.db.request_count", &[("op", "get")]);
        assert!(get_metric.is_some());

        let scan_metric =
            snapshot.by_name_and_labels("slatedb.db.request_count", &[("op", "scan")]);
        assert!(scan_metric.is_some());

        let missing = snapshot.by_name_and_labels("slatedb.db.request_count", &[("op", "delete")]);
        assert!(missing.is_none());
    }

    #[test]
    fn should_canonicalize_label_order() {
        let recorder = DefaultMetricsRecorder::new();
        let counter = recorder.register_counter("test.labeled", "test", &[("b", "2"), ("a", "1")]);
        counter.increment(1);

        let snapshot = recorder.snapshot();
        // Labels should be sorted by key
        let metric = &snapshot.by_name("test.labeled")[0];
        assert_eq!(
            metric.labels,
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );

        // Lookup should work regardless of input order
        let found = snapshot.by_name_and_labels("test.labeled", &[("a", "1"), ("b", "2")]);
        assert!(found.is_some());
        let found = snapshot.by_name_and_labels("test.labeled", &[("b", "2"), ("a", "1")]);
        assert!(found.is_some());
    }

    #[test]
    fn should_forward_to_composite() {
        let default = Arc::new(DefaultMetricsRecorder::new());
        let user = Arc::new(DefaultMetricsRecorder::new());
        let composite = CompositeMetricsRecorder::new(
            default.clone(),
            Some(user.clone() as Arc<dyn MetricsRecorder>),
        );

        let counter = composite.register_counter("test.counter", "test", &[]);
        counter.increment(10);

        // Both recorders should see the value
        let default_snapshot = default.snapshot();
        let user_snapshot = user.snapshot();
        match &default_snapshot.by_name("test.counter")[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 10),
            other => panic!("expected Counter, got {:?}", other),
        }
        match &user_snapshot.by_name("test.counter")[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 10),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_work_without_user_recorder() {
        let default = Arc::new(DefaultMetricsRecorder::new());
        let composite = CompositeMetricsRecorder::new(default.clone(), None);

        let gauge = composite.register_gauge("test.gauge", "test", &[]);
        gauge.set(99.0);

        let snapshot = composite.snapshot();
        match &snapshot.by_name("test.gauge")[0].value {
            MetricValue::Gauge(v) => assert!((v - 99.0).abs() < f64::EPSILON),
            other => panic!("expected Gauge, got {:?}", other),
        }
    }
}
