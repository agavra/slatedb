//! Registry approach: SlateDB owns all metric storage, exporters read from it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ============================================================================
// Core types — SlateDB owns all metric storage
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetricKey {
    pub name: String,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone)]
pub enum MetricHandle {
    Counter(Arc<AtomicU64>),
    Gauge(Arc<AtomicU64>), // f64 via to_bits/from_bits
    Histogram {
        count: Arc<AtomicU64>,
        sum: Arc<AtomicU64>,
        min: Arc<AtomicU64>,
        max: Arc<AtomicU64>,
    },
}

pub struct MetricDescriptor {
    pub key: MetricKey,
    pub description: String,
    pub kind: MetricKind,
    pub handle: MetricHandle,
}

// ============================================================================
// MetricsRegistry
// ============================================================================

pub struct MetricsRegistry {
    metrics: Mutex<Vec<MetricDescriptor>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            metrics: Mutex::new(Vec::new()),
        }
    }

    pub fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> RegistryCounter {
        let atom = Arc::new(AtomicU64::new(0));
        self.metrics.lock().unwrap().push(MetricDescriptor {
            key: canonicalize(name, labels),
            description: description.to_string(),
            kind: MetricKind::Counter,
            handle: MetricHandle::Counter(atom.clone()),
        });
        RegistryCounter(atom)
    }

    pub fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> RegistryGauge {
        let atom = Arc::new(AtomicU64::new(0f64.to_bits()));
        self.metrics.lock().unwrap().push(MetricDescriptor {
            key: canonicalize(name, labels),
            description: description.to_string(),
            kind: MetricKind::Gauge,
            handle: MetricHandle::Gauge(atom.clone()),
        });
        RegistryGauge(atom)
    }

    pub fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> RegistryHistogram {
        let count = Arc::new(AtomicU64::new(0));
        let sum = Arc::new(AtomicU64::new(0f64.to_bits()));
        let min = Arc::new(AtomicU64::new(f64::MAX.to_bits()));
        let max = Arc::new(AtomicU64::new(f64::MIN.to_bits()));
        self.metrics.lock().unwrap().push(MetricDescriptor {
            key: canonicalize(name, labels),
            description: description.to_string(),
            kind: MetricKind::Histogram,
            handle: MetricHandle::Histogram {
                count: count.clone(),
                sum: sum.clone(),
                min: min.clone(),
                max: max.clone(),
            },
        });
        RegistryHistogram { count, sum, min, max }
    }

    /// Snapshot for db.metrics(). Clean, same as recorder approach.
    pub fn snapshot(&self) -> Vec<MetricSnapshot> {
        let guard = self.metrics.lock().unwrap();
        guard
            .iter()
            .map(|desc| MetricSnapshot {
                name: desc.key.name.clone(),
                labels: desc.key.labels.clone(),
                description: desc.description.clone(),
                value: match &desc.handle {
                    MetricHandle::Counter(a) => {
                        MetricValue::Counter(a.load(Ordering::Relaxed))
                    }
                    MetricHandle::Gauge(a) => {
                        MetricValue::Gauge(f64::from_bits(a.load(Ordering::Relaxed)))
                    }
                    MetricHandle::Histogram { count, sum, min, max } => {
                        MetricValue::Histogram {
                            count: count.load(Ordering::Relaxed),
                            sum: f64::from_bits(sum.load(Ordering::Relaxed)),
                            min: f64::from_bits(min.load(Ordering::Relaxed)),
                            max: f64::from_bits(max.load(Ordering::Relaxed)),
                        }
                    }
                },
            })
            .collect()
    }

    /// Provide read access to the raw handles for exporters.
    pub fn descriptors(&self) -> DescriptorSnapshot {
        let guard = self.metrics.lock().unwrap();
        DescriptorSnapshot {
            entries: guard
                .iter()
                .map(|d| DescriptorEntry {
                    name: d.key.name.clone(),
                    labels: d.key.labels.clone(),
                    description: d.description.clone(),
                    kind: d.kind,
                    handle: d.handle.clone(),
                })
                .collect(),
        }
    }
}

pub struct DescriptorSnapshot {
    pub entries: Vec<DescriptorEntry>,
}

pub struct DescriptorEntry {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub description: String,
    pub kind: MetricKind,
    pub handle: MetricHandle,
}

// ============================================================================
// Typed handles — concrete types, no vtable
// ============================================================================

#[derive(Clone)]
pub struct RegistryCounter(Arc<AtomicU64>);

impl RegistryCounter {
    pub fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct RegistryGauge(Arc<AtomicU64>);

impl RegistryGauge {
    pub fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
    pub fn increment(&self, delta: f64) {
        loop {
            let cur = self.0.load(Ordering::Relaxed);
            let new = f64::from_bits(cur) + delta;
            if self
                .0
                .compare_exchange_weak(cur, new.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
}

#[derive(Clone)]
pub struct RegistryHistogram {
    count: Arc<AtomicU64>,
    sum: Arc<AtomicU64>,
    min: Arc<AtomicU64>,
    max: Arc<AtomicU64>,
}

impl RegistryHistogram {
    pub fn record(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        // CAS for sum
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
        // CAS for min
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
        // CAS for max
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

// ============================================================================
// Snapshot types
// ============================================================================

pub struct MetricSnapshot {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub description: String,
    pub value: MetricValue,
}

pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram { count: u64, sum: f64, min: f64, max: f64 },
}

// ============================================================================
// Prometheus integration — read from SlateDB's atomics on scrape
// ============================================================================

pub mod prom {
    use super::*;
    use prometheus_client::encoding::{
        EncodeMetric, MetricEncoder, EncodeLabelSet,
    };
    use prometheus_client::metrics::MetricType;
    use prometheus_client::registry::Registry;

    // -- Custom EncodeMetric impls that read from SlateDB's atomics -----------

    /// Adapter: reads a SlateDB counter's AtomicU64 on Prometheus scrape.
    #[derive(Debug)]
    pub struct CounterAdapter(pub Arc<AtomicU64>);

    impl EncodeMetric for CounterAdapter {
        fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
            let value = self.0.load(Ordering::Relaxed);
            encoder.encode_counter::<Vec<(String, String)>, _, u64>(&value, None)
        }
        fn metric_type(&self) -> MetricType {
            MetricType::Counter
        }
    }

    /// Adapter: reads a SlateDB gauge's AtomicU64 (f64 bits) on scrape.
    #[derive(Debug)]
    pub struct GaugeAdapter(pub Arc<AtomicU64>);

    impl EncodeMetric for GaugeAdapter {
        fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
            let value = f64::from_bits(self.0.load(Ordering::Relaxed));
            encoder.encode_gauge(&value)
        }
        fn metric_type(&self) -> MetricType {
            MetricType::Gauge
        }
    }

    // Histogram adapter is harder — prometheus-client expects bucket boundaries
    // and counts, but our registry only has count/sum/min/max. We can only
    // expose it as a set of gauges, not as a native Prometheus histogram.
    //
    // This is the fundamental limitation: without bucket data in the registry,
    // Prometheus histograms don't work. Users who need real histograms have
    // no way to get them from the registry alone.

    // -- Bridge function: group by name, register as labeled families ----------

    /// Bridge SlateDB metrics into a prometheus-client Registry.
    ///
    /// This is where the complexity lives. We need to:
    /// 1. Group descriptors by (name, kind)
    /// 2. For each group, create a single EncodeMetric impl that encodes
    ///    all labeled variants as one metric family
    /// 3. Register that family with the prometheus registry
    pub fn bridge_to_prometheus(
        slatedb_registry: &MetricsRegistry,
        prom_registry: &mut Registry,
    ) {
        let snapshot = slatedb_registry.descriptors();

        // Group by (name, kind) -> Vec<(labels, handle)>
        let mut counter_groups: std::collections::HashMap<
            String,
            Vec<(Vec<(String, String)>, Arc<AtomicU64>, String)>,
        > = std::collections::HashMap::new();
        let mut gauge_groups: std::collections::HashMap<
            String,
            Vec<(Vec<(String, String)>, Arc<AtomicU64>, String)>,
        > = std::collections::HashMap::new();

        for entry in &snapshot.entries {
            let prom_name = entry.name.replace('.', "_");
            match &entry.handle {
                MetricHandle::Counter(atom) => {
                    counter_groups
                        .entry(prom_name)
                        .or_default()
                        .push((entry.labels.clone(), atom.clone(), entry.description.clone()));
                }
                MetricHandle::Gauge(atom) => {
                    gauge_groups
                        .entry(prom_name)
                        .or_default()
                        .push((entry.labels.clone(), atom.clone(), entry.description.clone()));
                }
                MetricHandle::Histogram { .. } => {
                    // Can't do native Prometheus histograms without buckets.
                    // Could register count/sum as separate metrics, but that's
                    // a poor experience. This is a real gap in the registry approach.
                }
            }
        }

        // Register each group as a metric family.
        for (name, members) in counter_groups {
            let desc = members.first().map(|m| m.2.clone()).unwrap_or_default();
            if members.iter().all(|(labels, _, _)| labels.is_empty()) {
                // Unlabeled: register directly
                if let Some((_, atom, _)) = members.into_iter().next() {
                    prom_registry.register(&name, &desc, CounterAdapter(atom));
                }
            } else {
                // Labeled: need a custom EncodeMetric that encodes all variants
                let family = LabeledCounterFamily {
                    members: members
                        .into_iter()
                        .map(|(labels, atom, _)| (labels, atom))
                        .collect(),
                };
                prom_registry.register(&name, &desc, family);
            }
        }

        for (name, members) in gauge_groups {
            let desc = members.first().map(|m| m.2.clone()).unwrap_or_default();
            if members.iter().all(|(labels, _, _)| labels.is_empty()) {
                if let Some((_, atom, _)) = members.into_iter().next() {
                    prom_registry.register(&name, &desc, GaugeAdapter(atom));
                }
            } else {
                let family = LabeledGaugeFamily {
                    members: members
                        .into_iter()
                        .map(|(labels, atom, _)| (labels, atom))
                        .collect(),
                };
                prom_registry.register(&name, &desc, family);
            }
        }
    }

    // -- Custom metric families for labeled metrics ---------------------------
    //
    // prometheus-client's Family<L, M> doesn't work here because it owns
    // the metric storage internally. We need to point at SlateDB's atomics.
    // So we implement EncodeMetric ourselves.

    #[derive(Debug)]
    struct LabeledCounterFamily {
        members: Vec<(Vec<(String, String)>, Arc<AtomicU64>)>,
    }

    impl EncodeMetric for LabeledCounterFamily {
        fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
            for (labels, atom) in &self.members {
                let value = atom.load(Ordering::Relaxed);
                let label_set = DynLabels(labels.clone());
                let mut family_encoder = encoder.encode_family(&label_set)?;
                family_encoder.encode_counter::<Vec<(String, String)>, _, u64>(&value, None)?;
            }
            Ok(())
        }
        fn metric_type(&self) -> MetricType {
            MetricType::Counter
        }
    }

    #[derive(Debug)]
    struct LabeledGaugeFamily {
        members: Vec<(Vec<(String, String)>, Arc<AtomicU64>)>,
    }

    impl EncodeMetric for LabeledGaugeFamily {
        fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
            for (labels, atom) in &self.members {
                let value = f64::from_bits(atom.load(Ordering::Relaxed));
                let label_set = DynLabels(labels.clone());
                let mut family_encoder = encoder.encode_family(&label_set)?;
                family_encoder.encode_gauge(&value)?;
            }
            Ok(())
        }
        fn metric_type(&self) -> MetricType {
            MetricType::Gauge
        }
    }

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct DynLabels(Vec<(String, String)>);

    impl EncodeLabelSet for DynLabels {
        fn encode(
            &self,
            encoder: &mut prometheus_client::encoding::LabelSetEncoder,
        ) -> Result<(), std::fmt::Error> {
            // Delegate to the blanket impl for Vec<(String, String)>
            self.0.encode(encoder)
        }
    }
}

// ============================================================================
// OpenTelemetry integration — needs polling + delta tracking
// ============================================================================

pub mod otel {
    use super::*;
    use opentelemetry::metrics::Meter;
    use opentelemetry::KeyValue;

    /// OTel exporter that periodically reads from the registry and forwards
    /// values to OTel instruments.
    ///
    /// This is fundamentally more complex than the recorder approach because:
    /// 1. OTel counters are additive (add delta), but our registry stores absolutes
    /// 2. OTel histograms expect individual record() calls, but we only have aggregates
    /// 3. We need a background task to poll
    pub struct OtelExporter {
        registry: Arc<MetricsRegistry>,
        meter: Meter,
        // Track previous counter values to compute deltas
        last_counter_values: Mutex<std::collections::HashMap<String, u64>>,
        // OTel instruments, created lazily on first export
        instruments: Mutex<Option<OtelInstruments>>,
    }

    struct OtelInstruments {
        counters: Vec<(
            String,
            Vec<(String, String)>,
            opentelemetry::metrics::Counter<u64>,
            Arc<AtomicU64>,
        )>,
        gauges: Vec<(
            String,
            Vec<(String, String)>,
            opentelemetry::metrics::Gauge<f64>,
            Arc<AtomicU64>,
        )>,
        // Histograms: we can only export count/sum as gauges.
        // Individual observations are lost — this is the key limitation.
    }

    impl OtelExporter {
        pub fn new(registry: Arc<MetricsRegistry>, meter: Meter) -> Self {
            Self {
                registry,
                meter,
                last_counter_values: Mutex::new(std::collections::HashMap::new()),
                instruments: Mutex::new(None),
            }
        }

        /// Must be called periodically (e.g., every 10-60s) to push metrics.
        pub fn export(&self) {
            let mut instruments_guard = self.instruments.lock().unwrap();

            // Lazy init: create OTel instruments from registry descriptors
            if instruments_guard.is_none() {
                let snapshot = self.registry.descriptors();
                let mut counters = Vec::new();
                let mut gauges = Vec::new();

                for entry in snapshot.entries {
                    match &entry.handle {
                        MetricHandle::Counter(atom) => {
                            let counter = self
                                .meter
                                .u64_counter(entry.name.clone())
                                .with_description(entry.description.clone())
                                .build();
                            counters.push((
                                entry.name.clone(),
                                entry.labels.clone(),
                                counter,
                                atom.clone(),
                            ));
                        }
                        MetricHandle::Gauge(atom) => {
                            let gauge = self
                                .meter
                                .f64_gauge(entry.name.clone())
                                .with_description(entry.description.clone())
                                .build();
                            gauges.push((
                                entry.name.clone(),
                                entry.labels.clone(),
                                gauge,
                                atom.clone(),
                            ));
                        }
                        MetricHandle::Histogram { .. } => {
                            // We can't reconstruct individual observations from
                            // count/sum/min/max. The best we could do is export
                            // these as separate gauges, which defeats the purpose
                            // of having histograms in OTel.
                            //
                            // This is where the registry approach fundamentally
                            // breaks down for OTel histograms.
                        }
                    }
                }

                *instruments_guard = Some(OtelInstruments { counters, gauges });
            }

            let instruments = instruments_guard.as_ref().unwrap();

            // Export counters (need delta calculation)
            let mut last_values = self.last_counter_values.lock().unwrap();
            for (name, labels, otel_counter, atom) in &instruments.counters {
                let current = atom.load(Ordering::Relaxed);
                let key = format!("{name}{labels:?}");
                let last = last_values.get(&key).copied().unwrap_or(0);
                let delta = current.saturating_sub(last);
                if delta > 0 {
                    let attrs: Vec<KeyValue> = labels
                        .iter()
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                        .collect();
                    otel_counter.add(delta, &attrs);
                }
                last_values.insert(key, current);
            }

            // Export gauges (absolute values, straightforward)
            for (_name, labels, otel_gauge, atom) in &instruments.gauges {
                let value = f64::from_bits(atom.load(Ordering::Relaxed));
                let attrs: Vec<KeyValue> = labels
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect();
                otel_gauge.record(value, &attrs);
            }
        }
    }

    // Usage would be:
    //
    //   let registry = Arc::new(MetricsRegistry::new());
    //   let provider = SdkMeterProvider::builder()...build();
    //   let exporter = OtelExporter::new(registry.clone(), provider.meter("slatedb"));
    //
    //   // Background task
    //   tokio::spawn(async move {
    //       let mut interval = tokio::time::interval(Duration::from_secs(15));
    //       loop {
    //           interval.tick().await;
    //           exporter.export();
    //       }
    //   });
    //
    // Compare with the recorder approach where this is just:
    //
    //   let db = Db::builder("my_db", object_store)
    //       .with_metrics_recorder(Arc::new(OtelRecorder::new(provider.meter("slatedb"))))
    //       .open().await?;
    //
    // No background task, no delta tracking, histograms just work.
}

// ============================================================================
// Helpers
// ============================================================================

fn canonicalize(name: &str, labels: &[(&str, &str)]) -> MetricKey {
    let mut sorted: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    sorted.sort();
    MetricKey {
        name: name.to_string(),
        labels: sorted,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct DbStats {
        get_requests: RegistryCounter,
        scan_requests: RegistryCounter,
        wal_buffer_estimated_bytes: RegistryGauge,
        get_latency: RegistryHistogram,
    }

    impl DbStats {
        fn new(registry: &MetricsRegistry) -> Self {
            Self {
                get_requests: registry.register_counter(
                    "slatedb.db.request_count",
                    "Number of DB requests",
                    &[("op", "get")],
                ),
                scan_requests: registry.register_counter(
                    "slatedb.db.request_count",
                    "Number of DB requests",
                    &[("op", "scan")],
                ),
                wal_buffer_estimated_bytes: registry.register_gauge(
                    "slatedb.db.wal_buffer_estimated_bytes",
                    "Estimated WAL buffer size in bytes",
                    &[],
                ),
                get_latency: registry.register_histogram(
                    "slatedb.db.request_duration_seconds",
                    "DB request latency",
                    &[("op", "get")],
                ),
            }
        }
    }

    #[test]
    fn should_record_and_snapshot_counters() {
        // given
        let registry = MetricsRegistry::new();
        let stats = DbStats::new(&registry);

        // when
        stats.get_requests.increment(1);
        stats.get_requests.increment(1);
        stats.get_requests.increment(1);
        stats.scan_requests.increment(1);

        // then
        let snapshot = registry.snapshot();
        let get_counter = snapshot
            .iter()
            .find(|m| {
                m.name == "slatedb.db.request_count"
                    && m.labels == vec![("op".to_string(), "get".to_string())]
            })
            .unwrap();
        assert!(matches!(get_counter.value, MetricValue::Counter(3)));

        let scan_counter = snapshot
            .iter()
            .find(|m| {
                m.name == "slatedb.db.request_count"
                    && m.labels == vec![("op".to_string(), "scan".to_string())]
            })
            .unwrap();
        assert!(matches!(scan_counter.value, MetricValue::Counter(1)));
    }

    #[test]
    fn should_snapshot_gauge() {
        // given
        let registry = MetricsRegistry::new();
        let stats = DbStats::new(&registry);

        // when
        stats.wal_buffer_estimated_bytes.set(1024.0);
        stats.wal_buffer_estimated_bytes.increment(512.0);
        stats.wal_buffer_estimated_bytes.increment(-256.0);

        // then
        let snapshot = registry.snapshot();
        let gauge = snapshot
            .iter()
            .find(|m| m.name == "slatedb.db.wal_buffer_estimated_bytes")
            .unwrap();
        match gauge.value {
            MetricValue::Gauge(v) => assert_eq!(v, 1280.0),
            _ => panic!("expected gauge"),
        }
    }
}
