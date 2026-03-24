//! Example: Prometheus metrics integration with SlateDB
//!
//! This example demonstrates how to implement a `MetricsRecorder` that bridges
//! SlateDB metrics to the `prometheus-client` crate, serve them over HTTP, and
//! then scrape the /metrics endpoint to verify everything works end-to-end.
//!
//! The key design point: each unique metric *name* maps to one prometheus-client
//! `Family`, and each unique (name, labels) registration materializes a labeled
//! child within that family. This mirrors how Prometheus data model works —
//! `slatedb_db_request_count{op="get"}` and `slatedb_db_request_count{op="scan"}`
//! are two time series inside the same metric family.
//!
//! Run with: cargo run -p examples --bin prometheus-metrics

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter as PromCounter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as PromGauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram as PromHistogram};
use prometheus_client::registry::Registry;

use slatedb::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder};

// ---------------------------------------------------------------------------
// Label type for prometheus-client Family
// ---------------------------------------------------------------------------

/// A sorted list of key-value pairs used as the label set for prometheus-client
/// `Family` lookups. Sorting ensures that two registrations with the same labels
/// in different order resolve to the same child metric.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PromLabels(Vec<(String, String)>);

impl prometheus_client::encoding::EncodeLabelSet for PromLabels {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        for (k, v) in &self.0 {
            use prometheus_client::encoding::EncodeLabelValue;
            let mut label = encoder.encode_label();
            let mut key_enc = label.encode_label_key()?;
            prometheus_client::encoding::EncodeLabelKey::encode(&k.as_str(), &mut key_enc)?;
            let mut val_enc = key_enc.encode_label_value()?;
            v.as_str().encode(&mut val_enc)?;
            val_enc.finish()?;
        }
        Ok(())
    }
}

impl PromLabels {
    fn from_slice(labels: &[(&str, &str)]) -> Self {
        let mut sorted: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        sorted.sort();
        Self(sorted)
    }
}

// ---------------------------------------------------------------------------
// Prometheus-backed metric handles
// ---------------------------------------------------------------------------

struct PrometheusCounter(PromCounter);

impl CounterFn for PrometheusCounter {
    fn increment(&self, value: u64) {
        self.0.inc_by(value);
    }
}

struct PrometheusGauge {
    gauge: PromGauge<f64, std::sync::atomic::AtomicU64>,
}

impl GaugeFn for PrometheusGauge {
    fn set(&self, value: f64) {
        self.gauge.set(value);
    }

    fn increment(&self, value: f64) {
        if value >= 0.0 {
            self.gauge.inc_by(value);
        } else {
            // prometheus-client Gauge doesn't have dec_by for f64,
            // so we do a CAS loop on the inner atomic.
            use std::sync::atomic::Ordering;
            let inner = self.gauge.inner();
            loop {
                let current = inner.load(Ordering::Relaxed);
                let current_val = f64::from_bits(current);
                let new_val = current_val + value;
                if inner
                    .compare_exchange_weak(
                        current,
                        new_val.to_bits(),
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
}

struct PrometheusHistogram(PromHistogram);

impl HistogramFn for PrometheusHistogram {
    fn record(&self, value: f64) {
        self.0.observe(value);
    }
}

// ---------------------------------------------------------------------------
// PrometheusRecorder
// ---------------------------------------------------------------------------

type GaugeFamily = Family<PromLabels, PromGauge<f64, std::sync::atomic::AtomicU64>>;

fn new_histogram() -> PromHistogram {
    PromHistogram::new(exponential_buckets(0.001, 2.0, 16))
}

type HistogramFamily = Family<PromLabels, PromHistogram, fn() -> PromHistogram>;

/// All mutable recorder state behind a single lock to avoid nested-lock issues.
struct RecorderInner {
    registry: Registry,
    counter_families: HashMap<String, Family<PromLabels, PromCounter>>,
    gauge_families: HashMap<String, GaugeFamily>,
    histogram_families: HashMap<String, HistogramFamily>,
}

/// A `MetricsRecorder` that bridges SlateDB metrics into `prometheus-client`.
///
/// Each unique metric name is registered once as a prometheus-client `Family`.
/// Subsequent registrations with the same name but different labels create new
/// labeled children within the existing family — exactly how Prometheus expects
/// multi-dimensional metrics to work.
struct PrometheusRecorder {
    inner: Mutex<RecorderInner>,
}

impl PrometheusRecorder {
    fn new() -> Self {
        Self {
            inner: Mutex::new(RecorderInner {
                registry: Registry::default(),
                counter_families: HashMap::new(),
                gauge_families: HashMap::new(),
                histogram_families: HashMap::new(),
            }),
        }
    }

    fn encode(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let mut buf = String::new();
        encode(&mut buf, &inner.registry).unwrap();
        buf
    }
}

/// Convert a dotted SlateDB metric name to a Prometheus-compatible name.
/// Prometheus metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*`.
fn to_prom_name(name: &str) -> String {
    name.replace('.', "_")
}

impl MetricsRecorder for PrometheusRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let prom_labels = PromLabels::from_slice(labels);
        let mut inner = self.inner.lock().unwrap();

        // If this is the first time we've seen this metric name, create a new
        // Family and register it with the prometheus registry. All subsequent
        // registrations with the same name (but different labels) reuse this family.
        if !inner.counter_families.contains_key(name) {
            let family = Family::<PromLabels, PromCounter>::default();
            inner
                .registry
                .register(to_prom_name(name), description, family.clone());
            inner.counter_families.insert(name.to_string(), family);
        }
        let family = &inner.counter_families[name];

        // Materialize the labeled child within the shared family.
        let counter = family.get_or_create(&prom_labels).clone();
        Arc::new(PrometheusCounter(counter))
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let prom_labels = PromLabels::from_slice(labels);
        let mut inner = self.inner.lock().unwrap();

        if !inner.gauge_families.contains_key(name) {
            let family = GaugeFamily::default();
            inner
                .registry
                .register(to_prom_name(name), description, family.clone());
            inner.gauge_families.insert(name.to_string(), family);
        }
        let family = &inner.gauge_families[name];

        let gauge = family.get_or_create(&prom_labels).clone();
        Arc::new(PrometheusGauge { gauge })
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn HistogramFn> {
        let prom_labels = PromLabels::from_slice(labels);
        let mut inner = self.inner.lock().unwrap();

        if !inner.histogram_families.contains_key(name) {
            let family = HistogramFamily::new_with_constructor(new_histogram as fn() -> _);
            inner
                .registry
                .register(to_prom_name(name), description, family.clone());
            inner.histogram_families.insert(name.to_string(), family);
        }
        let family = &inner.histogram_families[name];

        let histogram = family.get_or_create(&prom_labels).clone();
        Arc::new(PrometheusHistogram(histogram))
    }
}

// ---------------------------------------------------------------------------
// HTTP server for /metrics endpoint
// ---------------------------------------------------------------------------

async fn serve_metrics(
    recorder: Arc<PrometheusRecorder>,
    addr: std::net::SocketAddr,
) -> tokio::task::JoinHandle<()> {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr).await.unwrap();
    println!(
        "Prometheus metrics server listening on http://{}/metrics",
        addr
    );

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            let recorder = recorder.clone();
            tokio::spawn(async move {
                let service = service_fn(move |_req: Request<hyper::body::Incoming>| {
                    let recorder = recorder.clone();
                    async move {
                        let body = recorder.encode();
                        Ok::<_, hyper::Error>(
                            Response::builder()
                                .header("content-type", "text/plain; charset=utf-8")
                                .body(Full::new(Bytes::from(body)))
                                .unwrap(),
                        )
                    }
                });
                let io = TokioIo::new(stream);
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    eprintln!("server error: {}", e);
                }
            });
        }
    })
}

// ---------------------------------------------------------------------------
// Main: wire everything together
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Create the Prometheus recorder
    let prom_recorder = Arc::new(PrometheusRecorder::new());

    // 2. Start the /metrics HTTP server
    let addr: std::net::SocketAddr = "127.0.0.1:9090".parse()?;
    let server_handle = serve_metrics(prom_recorder.clone(), addr).await;

    // 3. Open SlateDB with the Prometheus recorder
    let object_store = Arc::new(object_store::memory::InMemory::new());
    let db = slatedb::Db::builder("test_prometheus", object_store)
        .with_metrics_recorder(prom_recorder.clone() as Arc<dyn MetricsRecorder>)
        .build()
        .await?;

    // 4. Do some DB operations to generate metrics
    println!("\n--- Performing DB operations ---");
    for i in 0..10u64 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).await?;
    }
    println!("Wrote 10 key-value pairs");

    for i in 0..5u64 {
        let key = format!("key_{}", i);
        let result = db.get(key.as_bytes()).await?;
        assert!(result.is_some());
    }
    println!("Read 5 keys");

    db.flush().await?;
    println!("Flushed WAL");

    // 5. Check db.metrics() (built-in default recorder)
    println!("\n--- SlateDB built-in metrics snapshot ---");
    let metrics = db.metrics();
    for m in metrics.all() {
        let label_str = if m.labels.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = m
                .labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        };
        match &m.value {
            slatedb::metrics::MetricValue::Counter(v) if *v > 0 => {
                println!("  {}{} = {} (counter)", m.name, label_str, v);
            }
            slatedb::metrics::MetricValue::Gauge(v) if *v != 0.0 => {
                println!("  {}{} = {} (gauge)", m.name, label_str, v);
            }
            slatedb::metrics::MetricValue::Histogram { count, .. } if *count > 0 => {
                println!("  {}{} count={} (histogram)", m.name, label_str, count);
            }
            _ => {}
        }
    }

    // 6. Scrape the Prometheus /metrics endpoint via HTTP
    println!("\n--- Scraping http://{}/metrics ---", addr);
    let resp = reqwest_lite_get(&addr).await?;
    // Print only the lines with slatedb metrics that have non-zero values
    println!();
    for line in resp.lines() {
        if line.starts_with('#') && line.contains("slatedb") {
            println!("{}", line);
        } else if line.starts_with("slatedb") && !line.ends_with(" 0") {
            println!("{}", line);
        }
    }

    // 7. Verify Prometheus output: labels should appear as children of a single family
    println!("\n--- Verifying Prometheus output ---");

    // The request_count family should contain both op="get" and op="flush" children
    // under a single # TYPE declaration (not two separate metric families).
    let type_count = resp
        .lines()
        .filter(|l| l.starts_with("# TYPE slatedb_db_request_count"))
        .count();
    assert_eq!(
        type_count, 1,
        "expected exactly 1 TYPE declaration for slatedb_db_request_count (family reuse), got {}",
        type_count
    );
    assert!(
        resp.contains(r#"slatedb_db_request_count_total{op="get"}"#),
        "expected op=get label in request_count family"
    );
    assert!(
        resp.contains(r#"slatedb_db_request_count_total{op="flush"}"#),
        "expected op=flush label in request_count family"
    );

    // Same check for the db_cache access_count family — multiple entry_kind/result combos
    let cache_type_count = resp
        .lines()
        .filter(|l| l.starts_with("# TYPE slatedb_db_cache_access_count"))
        .count();
    assert_eq!(
        cache_type_count, 1,
        "expected exactly 1 TYPE declaration for slatedb_db_cache_access_count, got {}",
        cache_type_count
    );

    assert!(
        resp.contains("slatedb_db_write_ops"),
        "expected slatedb_db_write_ops in Prometheus output"
    );

    println!("All assertions passed!");

    // Cleanup
    db.close().await?;
    server_handle.abort();

    Ok(())
}

/// Minimal HTTP GET using just tokio TCP (avoids adding reqwest as a dependency).
async fn reqwest_lite_get(addr: &std::net::SocketAddr) -> anyhow::Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(addr).await?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let response = String::from_utf8_lossy(&buf).to_string();

    // Strip HTTP headers, return just the body
    if let Some(pos) = response.find("\r\n\r\n") {
        Ok(response[pos + 4..].to_string())
    } else {
        Ok(response)
    }
}
