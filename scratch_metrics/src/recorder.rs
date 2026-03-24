//! Recorder approach: mock recorder + real OTel integration.
//!
//! This compiles against real opentelemetry + prometheus-client crates.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ============================================================================
// Core traits (from the RFC)
// ============================================================================

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
    fn increment(&self, value: f64); // negative to decrement
}

pub trait HistogramFn: Send + Sync {
    fn record(&self, value: f64);
}

// ============================================================================
// Default (atomic-backed) recorder — always present for db.metrics()
// ============================================================================

pub struct DefaultCounter(AtomicU64);
impl CounterFn for DefaultCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

pub struct DefaultGauge(AtomicU64);
impl GaugeFn for DefaultGauge {
    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
    fn increment(&self, delta: f64) {
        loop {
            let current = self.0.load(Ordering::Relaxed);
            let new = f64::from_bits(current) + delta;
            if self
                .0
                .compare_exchange_weak(
                    current,
                    new.to_bits(),
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

pub struct DefaultHistogram {
    count: AtomicU64,
    sum: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}
impl HistogramFn for DefaultHistogram {
    fn record(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        // sum CAS
        loop {
            let cur = self.sum.load(Ordering::Relaxed);
            let new = f64::from_bits(cur) + value;
            if self
                .sum
                .compare_exchange_weak(cur, new.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        // min CAS
        loop {
            let cur = self.min.load(Ordering::Relaxed);
            if value >= f64::from_bits(cur) {
                break;
            }
            if self
                .min
                .compare_exchange_weak(cur, value.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        // max CAS
        loop {
            let cur = self.max.load(Ordering::Relaxed);
            if value <= f64::from_bits(cur) {
                break;
            }
            if self
                .max
                .compare_exchange_weak(cur, value.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

pub struct DefaultMetricsRecorder {
    pub entries: Mutex<Vec<DefaultMetricEntry>>,
}

pub struct DefaultMetricEntry {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub description: String,
    pub kind: DefaultMetricKind,
}

pub enum DefaultMetricKind {
    Counter(Arc<DefaultCounter>),
    Gauge(Arc<DefaultGauge>),
    Histogram(Arc<DefaultHistogram>),
}

impl DefaultMetricsRecorder {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let c = Arc::new(DefaultCounter(AtomicU64::new(0)));
        self.entries.lock().unwrap().push(DefaultMetricEntry {
            name: name.to_string(),
            labels: canonicalize(labels),
            description: description.to_string(),
            kind: DefaultMetricKind::Counter(c.clone()),
        });
        c
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let g = Arc::new(DefaultGauge(AtomicU64::new(0f64.to_bits())));
        self.entries.lock().unwrap().push(DefaultMetricEntry {
            name: name.to_string(),
            labels: canonicalize(labels),
            description: description.to_string(),
            kind: DefaultMetricKind::Gauge(g.clone()),
        });
        g
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        let h = Arc::new(DefaultHistogram {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0f64.to_bits()),
            min: AtomicU64::new(f64::MAX.to_bits()),
            max: AtomicU64::new(f64::MIN.to_bits()),
        });
        self.entries.lock().unwrap().push(DefaultMetricEntry {
            name: name.to_string(),
            labels: canonicalize(labels),
            description: description.to_string(),
            kind: DefaultMetricKind::Histogram(h.clone()),
        });
        h
    }
}

// ============================================================================
// Composite recorder
// ============================================================================

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

pub struct CompositeMetricsRecorder {
    pub default: DefaultMetricsRecorder,
    pub user: Option<Arc<dyn MetricsRecorder>>,
}

impl MetricsRecorder for CompositeMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let default = self.default.register_counter(name, description, labels);
        let user = self
            .user
            .as_ref()
            .map(|u| u.register_counter(name, description, labels));
        Arc::new(CompositeCounter { default, user })
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let default = self.default.register_gauge(name, description, labels);
        let user = self
            .user
            .as_ref()
            .map(|u| u.register_gauge(name, description, labels));
        Arc::new(CompositeGauge { default, user })
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        let default = self.default.register_histogram(name, description, labels);
        let user = self
            .user
            .as_ref()
            .map(|u| u.register_histogram(name, description, labels));
        Arc::new(CompositeHistogram { default, user })
    }
}

// ============================================================================
// Mock recorder (for testing)
// ============================================================================

pub struct MockRecorder {
    counters: Mutex<HashMap<String, Arc<MockCounter>>>,
    gauges: Mutex<HashMap<String, Arc<MockGauge>>>,
    histograms: Mutex<HashMap<String, Arc<MockHistogram>>>,
}

struct MockCounter {
    total: AtomicU64,
}

struct MockGauge {
    current: Mutex<f64>,
}

struct MockHistogram {
    recordings: Mutex<Vec<f64>>,
}

impl MockRecorder {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
            gauges: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
        }
    }

    pub fn counter_total(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let key = make_key(name, labels);
        self.counters
            .lock()
            .unwrap()
            .get(&key)
            .map(|c| c.total.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn gauge_value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let key = make_key(name, labels);
        self.gauges
            .lock()
            .unwrap()
            .get(&key)
            .map(|g| *g.current.lock().unwrap())
    }

    pub fn histogram_recordings(&self, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
        let key = make_key(name, labels);
        self.histograms
            .lock()
            .unwrap()
            .get(&key)
            .map(|h| h.recordings.lock().unwrap().clone())
            .unwrap_or_default()
    }
}

impl CounterFn for MockCounter {
    fn increment(&self, value: u64) {
        self.total.fetch_add(value, Ordering::Relaxed);
    }
}

impl GaugeFn for MockGauge {
    fn set(&self, value: f64) {
        *self.current.lock().unwrap() = value;
    }
    fn increment(&self, delta: f64) {
        *self.current.lock().unwrap() += delta;
    }
}

impl HistogramFn for MockHistogram {
    fn record(&self, value: f64) {
        self.recordings.lock().unwrap().push(value);
    }
}

impl MetricsRecorder for MockRecorder {
    fn register_counter(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let key = make_key(name, labels);
        let c = Arc::new(MockCounter {
            total: AtomicU64::new(0),
        });
        self.counters.lock().unwrap().insert(key, c.clone());
        c
    }

    fn register_gauge(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let key = make_key(name, labels);
        let g = Arc::new(MockGauge {
            current: Mutex::new(0.0),
        });
        self.gauges.lock().unwrap().insert(key, g.clone());
        g
    }

    fn register_histogram(
        &self,
        name: &str,
        _description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        let key = make_key(name, labels);
        let h = Arc::new(MockHistogram {
            recordings: Mutex::new(Vec::new()),
        });
        self.histograms.lock().unwrap().insert(key, h.clone());
        h
    }
}

// ============================================================================
// OpenTelemetry integration (real imports)
// ============================================================================

pub mod otel {
    use super::*;
    use opentelemetry::metrics::Meter;
    use opentelemetry::KeyValue;

    /// OTel adapter: wraps an OTel Counter<u64> as a SlateDB CounterFn.
    struct OtelCounter {
        counter: opentelemetry::metrics::Counter<u64>,
        attrs: Vec<KeyValue>,
    }

    impl CounterFn for OtelCounter {
        fn increment(&self, value: u64) {
            self.counter.add(value, &self.attrs);
        }
    }

    /// OTel adapter: wraps an OTel Gauge<f64> as a SlateDB GaugeFn.
    ///
    /// OTel Gauge only has record() (replaces value). For increment(), we keep
    /// local atomic state and record() the new absolute value after each add.
    struct OtelGauge {
        gauge: opentelemetry::metrics::Gauge<f64>,
        attrs: Vec<KeyValue>,
        current: AtomicU64, // f64 bits — local state for increment()
    }

    impl GaugeFn for OtelGauge {
        fn set(&self, value: f64) {
            self.current.store(value.to_bits(), Ordering::Relaxed);
            self.gauge.record(value, &self.attrs);
        }

        fn increment(&self, delta: f64) {
            loop {
                let cur = self.current.load(Ordering::Relaxed);
                let new_val = f64::from_bits(cur) + delta;
                if self
                    .current
                    .compare_exchange_weak(
                        cur,
                        new_val.to_bits(),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.gauge.record(new_val, &self.attrs);
                    break;
                }
            }
        }
    }

    /// OTel adapter: wraps an OTel Histogram<f64> as a SlateDB HistogramFn.
    struct OtelHistogram {
        histogram: opentelemetry::metrics::Histogram<f64>,
        attrs: Vec<KeyValue>,
    }

    impl HistogramFn for OtelHistogram {
        fn record(&self, value: f64) {
            self.histogram.record(value, &self.attrs);
        }
    }

    // -- The recorder ---------------------------------------------------------

    pub struct OtelRecorder {
        meter: Meter,
    }

    impl OtelRecorder {
        pub fn new(meter: Meter) -> Self {
            Self { meter }
        }
    }

    impl MetricsRecorder for OtelRecorder {
        fn register_counter(
            &self,
            name: &str,
            desc: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn CounterFn> {
            // OTel builders require owned Strings (impl Into<Cow<'static, str>>)
            let counter = self
                .meter
                .u64_counter(name.to_string())
                .with_description(desc.to_string())
                .build();
            Arc::new(OtelCounter {
                counter,
                attrs: to_kv(labels),
            })
        }

        fn register_gauge(
            &self,
            name: &str,
            desc: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn GaugeFn> {
            let gauge = self
                .meter
                .f64_gauge(name.to_string())
                .with_description(desc.to_string())
                .build();
            Arc::new(OtelGauge {
                gauge,
                attrs: to_kv(labels),
                current: AtomicU64::new(0f64.to_bits()),
            })
        }

        fn register_histogram(
            &self,
            name: &str,
            desc: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn HistogramFn> {
            let histogram = self
                .meter
                .f64_histogram(name.to_string())
                .with_description(desc.to_string())
                .build();
            Arc::new(OtelHistogram {
                histogram,
                attrs: to_kv(labels),
            })
        }
    }

    fn to_kv(labels: &[(&str, &str)]) -> Vec<KeyValue> {
        labels
            .iter()
            .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
            .collect()
    }
}

// ============================================================================
// Prometheus integration (real imports)
// ============================================================================

pub mod prom {
    use super::*;
    use prometheus_client::encoding::EncodeLabelSet;
    use prometheus_client::metrics::counter::Counter as PromCounter;
    use prometheus_client::metrics::family::Family;
    use prometheus_client::metrics::gauge::Gauge as PromGauge;
    use prometheus_client::metrics::histogram::Histogram as PromHistogram;
    use prometheus_client::registry::Registry;

    // Newtype so we can implement Hash/Eq for Family's label set.
    // Vec<(String, String)> already implements EncodeLabelSet via the blanket
    // impl: Vec<T: EncodeLabel> + (K: EncodeLabelKey, V: EncodeLabelValue).
    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct PromLabels(Vec<(String, String)>);

    impl EncodeLabelSet for PromLabels {
        fn encode(
            &self,
            encoder: &mut prometheus_client::encoding::LabelSetEncoder,
        ) -> Result<(), std::fmt::Error> {
            // Delegate to the blanket impl for Vec<(String, String)>
            self.0.encode(encoder)
        }
    }

    // -- Adapters: wrap prometheus-client types to implement SlateDB traits ----

    struct PromCounterAdapter(PromCounter);

    impl CounterFn for PromCounterAdapter {
        fn increment(&self, value: u64) {
            self.0.inc_by(value);
        }
    }

    struct PromGaugeAdapter(PromGauge<f64, AtomicU64>);

    impl GaugeFn for PromGaugeAdapter {
        fn set(&self, value: f64) {
            self.0.set(value);
        }
        fn increment(&self, delta: f64) {
            self.0.inc_by(delta);
        }
    }

    struct PromHistogramAdapter(PromHistogram);

    impl HistogramFn for PromHistogramAdapter {
        fn record(&self, value: f64) {
            self.0.observe(value);
        }
    }

    // -- The recorder ---------------------------------------------------------

    /// Prometheus recorder that creates Family-based metrics.
    ///
    /// Each unique metric name maps to one Family in the Prometheus registry.
    /// Each (name, labels) registration becomes a labeled child in that Family.
    pub struct PrometheusRecorder {
        registry: Mutex<Registry>,
        counters: Mutex<HashMap<String, Family<PromLabels, PromCounter>>>,
        gauges: Mutex<HashMap<String, Family<PromLabels, PromGauge<f64, AtomicU64>>>>,
        histograms: Mutex<HashMap<String, Family<PromLabels, PromHistogram>>>,
    }

    impl PrometheusRecorder {
        pub fn new() -> Self {
            Self {
                registry: Mutex::new(Registry::default()),
                counters: Mutex::new(HashMap::new()),
                gauges: Mutex::new(HashMap::new()),
                histograms: Mutex::new(HashMap::new()),
            }
        }

        /// Expose the inner registry for scraping (e.g., via an HTTP handler).
        pub fn registry(&self) -> &Mutex<Registry> {
            &self.registry
        }
    }

    impl MetricsRecorder for PrometheusRecorder {
        fn register_counter(
            &self,
            name: &str,
            desc: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn CounterFn> {
            let prom_name = name.replace('.', "_");
            let prom_labels = PromLabels(canonicalize(labels));

            let mut families = self.counters.lock().unwrap();
            let family = families.entry(prom_name.clone()).or_insert_with(|| {
                let family = Family::<PromLabels, PromCounter>::default();
                self.registry
                    .lock()
                    .unwrap()
                    .register(&prom_name, desc, family.clone());
                family
            });

            let counter = family.get_or_create(&prom_labels).clone();
            Arc::new(PromCounterAdapter(counter))
        }

        fn register_gauge(
            &self,
            name: &str,
            desc: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn GaugeFn> {
            let prom_name = name.replace('.', "_");
            let prom_labels = PromLabels(canonicalize(labels));

            let mut families = self.gauges.lock().unwrap();
            let family = families.entry(prom_name.clone()).or_insert_with(|| {
                let family = Family::<PromLabels, PromGauge<f64, AtomicU64>>::default();
                self.registry
                    .lock()
                    .unwrap()
                    .register(&prom_name, desc, family.clone());
                family
            });

            let gauge = family.get_or_create(&prom_labels).clone();
            Arc::new(PromGaugeAdapter(gauge))
        }

        fn register_histogram(
            &self,
            name: &str,
            desc: &str,
            labels: &[(&str, &str)],
        ) -> Arc<dyn HistogramFn> {
            let prom_name = name.replace('.', "_");
            let prom_labels = PromLabels(canonicalize(labels));

            let mut families = self.histograms.lock().unwrap();
            let family = families.entry(prom_name.clone()).or_insert_with(|| {
                let family = Family::<PromLabels, PromHistogram>::new_with_constructor(|| {
                    PromHistogram::new(
                        [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0]
                            .into_iter(),
                    )
                });
                self.registry
                    .lock()
                    .unwrap()
                    .register(&prom_name, desc, family.clone());
                family
            });

            let histogram = family.get_or_create(&prom_labels).clone();
            Arc::new(PromHistogramAdapter(histogram))
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn canonicalize(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut sorted: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    sorted.sort();
    sorted
}

fn make_key(name: &str, labels: &[(&str, &str)]) -> String {
    let mut sorted: Vec<_> = labels.to_vec();
    sorted.sort();
    let label_str: Vec<String> = sorted.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{name}{{{}}}", label_str.join(","))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Simulates what DbStats would look like.
    struct DbStats {
        get_requests: Arc<dyn CounterFn>,
        scan_requests: Arc<dyn CounterFn>,
        wal_buffer_estimated_bytes: Arc<dyn GaugeFn>,
        get_latency: Arc<dyn HistogramFn>,
    }

    impl DbStats {
        fn new(recorder: &dyn MetricsRecorder) -> Self {
            Self {
                get_requests: recorder.register_counter(
                    "slatedb.db.request_count",
                    "Number of DB requests",
                    &[("op", "get")],
                ),
                scan_requests: recorder.register_counter(
                    "slatedb.db.request_count",
                    "Number of DB requests",
                    &[("op", "scan")],
                ),
                wal_buffer_estimated_bytes: recorder.register_gauge(
                    "slatedb.db.wal_buffer_estimated_bytes",
                    "Estimated WAL buffer size in bytes",
                    &[],
                ),
                get_latency: recorder.register_histogram(
                    "slatedb.db.request_duration_seconds",
                    "DB request latency",
                    &[("op", "get")],
                ),
            }
        }
    }

    #[test]
    fn should_record_counters_via_mock() {
        // given
        let mock = Arc::new(MockRecorder::new());
        let composite = CompositeMetricsRecorder {
            default: DefaultMetricsRecorder::new(),
            user: Some(mock.clone()),
        };
        let stats = DbStats::new(&composite);

        // when
        stats.get_requests.increment(1);
        stats.get_requests.increment(1);
        stats.get_requests.increment(1);
        stats.scan_requests.increment(1);

        // then
        assert_eq!(
            mock.counter_total("slatedb.db.request_count", &[("op", "get")]),
            3
        );
        assert_eq!(
            mock.counter_total("slatedb.db.request_count", &[("op", "scan")]),
            1
        );
    }

    #[test]
    fn should_track_gauge_set_and_increment() {
        // given
        let mock = Arc::new(MockRecorder::new());
        let composite = CompositeMetricsRecorder {
            default: DefaultMetricsRecorder::new(),
            user: Some(mock.clone()),
        };
        let stats = DbStats::new(&composite);

        // when
        stats.wal_buffer_estimated_bytes.set(1024.0);
        stats.wal_buffer_estimated_bytes.increment(512.0);
        stats.wal_buffer_estimated_bytes.increment(-256.0);

        // then
        assert_eq!(
            mock.gauge_value("slatedb.db.wal_buffer_estimated_bytes", &[]),
            Some(1280.0) // 1024 + 512 - 256
        );
    }

    #[test]
    fn should_record_histogram_values() {
        // given
        let mock = Arc::new(MockRecorder::new());
        let composite = CompositeMetricsRecorder {
            default: DefaultMetricsRecorder::new(),
            user: Some(mock.clone()),
        };
        let stats = DbStats::new(&composite);

        // when
        stats.get_latency.record(0.001);
        stats.get_latency.record(0.005);
        stats.get_latency.record(0.1);

        // then
        let recordings =
            mock.histogram_recordings("slatedb.db.request_duration_seconds", &[("op", "get")]);
        assert_eq!(recordings, vec![0.001, 0.005, 0.1]);
    }

    #[test]
    fn should_work_without_user_recorder() {
        // given — no user recorder, just the default
        let composite = CompositeMetricsRecorder {
            default: DefaultMetricsRecorder::new(),
            user: None,
        };
        let stats = DbStats::new(&composite);

        // when
        stats.get_requests.increment(5);

        // then — default recorder still tracks it (would be visible via db.metrics())
        let entries = composite.default.entries.lock().unwrap();
        let counter_entry = entries
            .iter()
            .find(|e| e.name == "slatedb.db.request_count" && e.labels == vec![("op".to_string(), "get".to_string())])
            .unwrap();
        match &counter_entry.kind {
            DefaultMetricKind::Counter(c) => {
                assert_eq!(c.0.load(Ordering::Relaxed), 5);
            }
            _ => panic!("expected counter"),
        }
    }
}
