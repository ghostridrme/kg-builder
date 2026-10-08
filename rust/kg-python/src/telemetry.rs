//! Application-owned JSON logging and optional bounded OTLP export.
use serde::{Deserialize, Serialize};
#[cfg(feature = "otel")]
use std::time::Duration;
use tracing_subscriber::{filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    pub filter: String,
    pub service_name: String,
    pub otlp_endpoint: Option<String>,
    pub export_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub trace_queue_size: usize,
    pub trace_batch_size: usize,
    pub trace_interval_ms: u64,
    pub metric_interval_ms: u64,
    pub sample_ratio: f64,
}
impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            filter: "info".into(),
            service_name: "kg-python".into(),
            otlp_endpoint: None,
            export_timeout_ms: 3000,
            shutdown_timeout_ms: 5000,
            trace_queue_size: 2048,
            trace_batch_size: 256,
            trace_interval_ms: 1000,
            metric_interval_ms: 10000,
            sample_ratio: 1.0,
        }
    }
}
impl std::fmt::Debug for TelemetryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryConfig")
            .field("otlp_enabled", &self.otlp_endpoint.is_some())
            .finish_non_exhaustive()
    }
}
impl TelemetryConfig {
    pub fn validate(&self) -> Result<(), String> {
        tracing_subscriber::EnvFilter::try_new(&self.filter)
            .map_err(|_| "invalid telemetry filter")?;
        if self.service_name.is_empty()
            || self.service_name.len() > 128
            || self
                .service_name
                .chars()
                .any(|c| !c.is_ascii_alphanumeric() && !"._-".contains(c))
        {
            return Err("telemetry service_name must be a bounded application identifier".into());
        }
        if !(1..=30000).contains(&self.export_timeout_ms)
            || !(1..=30000).contains(&self.shutdown_timeout_ms)
            || !(1..=65536).contains(&self.trace_queue_size)
            || !(1..=self.trace_queue_size.min(4096)).contains(&self.trace_batch_size)
            || !(100..=60000).contains(&self.trace_interval_ms)
            || !(100..=300000).contains(&self.metric_interval_ms)
            || !self.sample_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.sample_ratio)
        {
            return Err("invalid telemetry limits".into());
        }
        if let Some(endpoint) = &self.otlp_endpoint {
            let url = url::Url::parse(endpoint).map_err(|_| "invalid telemetry endpoint")?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || !matches!(url.path(), "" | "/")
            {
                return Err(
                    "telemetry endpoint must be an HTTP(S) collector origin without credentials"
                        .into(),
                );
            }
            if !cfg!(feature = "otel") {
                return Err("OTLP export requires the otel Cargo feature".into());
            }
        }
        Ok(())
    }
}

fn application(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.target().starts_with("kg")
}
fn loggable(metadata: &tracing::Metadata<'_>) -> bool {
    application(metadata) && metadata.target() != "kg::metrics"
}
#[cfg(feature = "otel")]
fn metric_event(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.is_event()
        && metadata.target() == "kg::metrics"
        && metadata.fields().iter().all(|field| {
            matches!(
                field.name(),
                "operation" | "stage" | "outcome" | "interrupted" | "slot"
            ) || field.name().starts_with("monotonic_counter.")
                || field.name().starts_with("histogram.")
                || field.name().starts_with("counter.")
        })
}
pub struct TelemetryGuard {
    #[cfg(feature = "otel")]
    providers: Option<(
        opentelemetry_sdk::trace::SdkTracerProvider,
        opentelemetry_sdk::metrics::SdkMeterProvider,
    )>,
    #[cfg(feature = "otel")]
    shutdown_timeout: Duration,
}
impl TelemetryGuard {
    pub async fn shutdown(self) {
        #[cfg(feature = "otel")]
        if let Some((traces, metrics)) = self.providers {
            let budget = self.shutdown_timeout;
            // Both provider implementations block: never hold up the ingestion executor.
            let (sent, work) = tokio::sync::oneshot::channel();
            let started = std::thread::Builder::new()
                .name("telemetry-shutdown".into())
                .spawn(move || {
                    let started = std::time::Instant::now();
                    let trace_result = traces.shutdown_with_timeout(budget / 2);
                    let metric_result =
                        metrics.shutdown_with_timeout(budget.saturating_sub(started.elapsed()));
                    let _ = sent.send(trace_result.is_ok() && metric_result.is_ok());
                });
            if started.is_err() || !matches!(tokio::time::timeout(budget, work).await, Ok(Ok(true)))
            {
                tracing::warn!(target:"kg_python::telemetry",outcome="export_failed","telemetry shutdown incomplete");
            }
        }
    }
}
pub fn init(config: &TelemetryConfig) -> Result<TelemetryGuard, String> {
    config.validate()?;
    let filter = tracing_subscriber::EnvFilter::try_new(&config.filter)
        .map_err(|_| "invalid telemetry filter")?;
    #[cfg(feature = "otel")]
    let trace_filter = filter.clone();
    let logs = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(std::io::stderr)
        .with_filter(filter)
        .with_filter(filter_fn(loggable));
    #[cfg(feature = "otel")]
    if config.otlp_endpoint.is_some() {
        use opentelemetry::trace::TracerProvider;
        let (traces, metrics) = providers(config)?;
        let tracer = traces.tracer("kg");
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(trace_filter)
            .with_filter(filter_fn(|metadata| {
                application(metadata) && metadata.target() != "kg::metrics"
            }));
        let metric_layer = tracing_opentelemetry::MetricsLayer::new(metrics.clone())
            .with_filter(filter_fn(metric_event));
        tracing_subscriber::registry()
            .with(logs)
            .with(layer)
            .with(metric_layer)
            .try_init()
            .map_err(|_| "telemetry subscriber already initialized")?;
        return Ok(TelemetryGuard {
            providers: Some((traces, metrics)),
            shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
        });
    }
    tracing_subscriber::registry()
        .with(logs)
        .try_init()
        .map_err(|_| "telemetry subscriber already initialized")?;
    Ok(TelemetryGuard {
        #[cfg(feature = "otel")]
        providers: None,
        #[cfg(feature = "otel")]
        shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
    })
}
#[cfg(feature = "otel")]
fn clear_headers(mut request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    *request.metadata_mut() = tonic::metadata::MetadataMap::new();
    Ok(request)
}
#[cfg(feature = "otel")]
fn providers(
    config: &TelemetryConfig,
) -> Result<
    (
        opentelemetry_sdk::trace::SdkTracerProvider,
        opentelemetry_sdk::metrics::SdkMeterProvider,
    ),
    String,
> {
    use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
    use opentelemetry_sdk::{
        metrics::{PeriodicReader, SdkMeterProvider, Temporality},
        trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanLimits},
        Resource,
    };
    tokio::runtime::Handle::try_current()
        .map_err(|_| "OTLP initialization requires an async runtime")?;
    let timeout = Duration::from_millis(config.export_timeout_ms);
    let endpoint = config
        .otlp_endpoint
        .as_ref()
        .ok_or("missing telemetry endpoint")?;
    let mut channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .map_err(|_| "invalid telemetry endpoint")?
        .connect_timeout(timeout)
        .timeout(timeout);
    if endpoint.starts_with("https:") {
        channel = channel
            .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())
            .map_err(|_| "invalid collector TLS configuration")?;
    }
    let channel = channel.connect_lazy();
    // OTLP merges environment headers even with a supplied channel. This collector
    // connection has no header configuration; strip inherited SDK credentials.
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_channel(channel.clone())
        .with_timeout(timeout)
        .with_compression(opentelemetry_otlp::Compression::Gzip)
        .with_interceptor(clear_headers)
        .build()
        .map_err(|_| "trace exporter initialization failed")?;
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_channel(channel)
        .with_timeout(timeout)
        .with_compression(opentelemetry_otlp::Compression::Gzip)
        .with_interceptor(clear_headers)
        .with_temporality(Temporality::Cumulative)
        .build()
        .map_err(|_| "metric exporter initialization failed")?;
    let resource = Resource::builder_empty()
        .with_service_name(config.service_name.clone())
        .build();
    let batch = BatchConfigBuilder::default()
        .with_max_queue_size(config.trace_queue_size)
        .with_max_export_batch_size(config.trace_batch_size)
        .with_scheduled_delay(Duration::from_millis(config.trace_interval_ms))
        .build();
    let traces = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sample_ratio,
        ))))
        .with_span_limits(SpanLimits::default())
        .with_span_processor(
            BatchSpanProcessor::builder(span_exporter)
                .with_batch_config(batch)
                .build(),
        )
        .build();
    let reader = PeriodicReader::builder(metric_exporter)
        .with_interval(Duration::from_millis(config.metric_interval_ms))
        .build();
    let metrics = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();
    Ok((traces, metrics))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_is_explicit_and_does_not_disclose_invalid_endpoints() {
        let mut config = TelemetryConfig::default();
        for endpoint in [
            "https://user:secret@collector",
            "https://collector/?token=secret",
            "file:///secret",
        ] {
            config.otlp_endpoint = Some(endpoint.into());
            assert!(config.validate().is_err());
            assert!(!format!("{config:?}").contains("secret"));
            assert!(!config.validate().unwrap_err().contains("secret"));
        }
        // No environment bootstrap exists in the native host; explicit defaults own export.
        assert!(TelemetryConfig::default().otlp_endpoint.is_none());
        assert!(serde_json::from_str::<TelemetryConfig>(r#"{"unknown":true}"#).is_err());
        let mut config = TelemetryConfig::default();
        config.trace_batch_size = config.trace_queue_size + 1;
        assert!(config.validate().is_err());
    }
    #[test]
    fn json_logs_exclude_sdk_details_and_raw_metric_events() {
        use std::io::Write;
        #[derive(Clone)]
        struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = Capture(output.clone());
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(move || writer.clone())
                .with_filter(filter_fn(loggable)),
        );
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target:"kg_python::telemetry",outcome="success","run complete");
            tracing::error!(target:"opentelemetry_otlp",endpoint="credential-secret","export error");
            tracing::event!(target:"kg::metrics",tracing::Level::INFO,{monotonic_counter.kg_operations=1u64,operation="run"});
        });
        let bytes = output.lock().unwrap();
        let line = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(line.lines().count(), 1);
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(record["fields"]["outcome"], "success");
        assert!(!line.contains("credential-secret"));
        assert!(!line.contains("monotonic_counter"));
    }
    #[cfg(feature = "otel")]
    #[test]
    fn inherited_export_headers_are_removed() {
        let mut request = tonic::Request::new(());
        request.metadata_mut().insert(
            "authorization",
            "Bearer-environment-secret".parse().unwrap(),
        );
        request
            .metadata_mut()
            .insert("x-tenant", "environment-tenant".parse().unwrap());
        assert!(clear_headers(request).unwrap().metadata().is_empty());
    }
    #[cfg(feature = "otel")]
    #[test]
    fn remote_parent_and_dropped_future_metrics_preserve_context_without_identity_labels() {
        use kg_core::telemetry::{OperationGuard, OperationKind};
        use opentelemetry::{
            propagation::TextMapPropagator,
            trace::{TraceContextExt, TracerProvider},
        };
        use opentelemetry_sdk::{
            metrics::{
                data::{AggregatedMetrics, MetricData},
                InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
            },
            propagation::TraceContextPropagator,
            trace::{InMemorySpanExporter, SdkTracerProvider},
        };
        use std::future::Future;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let span_export = InMemorySpanExporter::default();
        let traces = SdkTracerProvider::builder()
            .with_simple_exporter(span_export.clone())
            .build();
        let metric_export = InMemoryMetricExporter::default();
        let metrics = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(metric_export.clone())
                    .with_interval(Duration::from_secs(3600))
                    .build(),
            )
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(traces.tracer("test")))
            .with(
                tracing_opentelemetry::MetricsLayer::new(metrics.clone())
                    .with_filter(filter_fn(metric_event)),
            );
        let headers = std::collections::HashMap::from([(
            "traceparent".into(),
            "00-11111111111111111111111111111111-2222222222222222-01".into(),
        )]);
        let remote = TraceContextPropagator::new().extract(&headers);
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!(target:"kg_python","request",org_id="private-org",run_id="private-run");
        span.set_parent(remote.clone()).unwrap();
        let mut future = Box::pin(async {
            let mut guard = OperationGuard::new(OperationKind::GraphRead);
            guard.waiting();
            std::future::pending::<()>().await;
        });
        span.in_scope(|| {
            assert!(future
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending())
        });
        drop(future);
        // Even an event on the metric target cannot smuggle unbounded attributes.
        tracing::event!(target:"kg::metrics",tracing::Level::INFO,{monotonic_counter.disallowed=1u64,org_id="private-org"});
        drop(span);
        traces.force_flush().unwrap();
        metrics.force_flush().unwrap();
        let spans = span_export.get_finished_spans().unwrap();
        let request = spans.iter().find(|span| span.name == "request").unwrap();
        assert_eq!(
            request.span_context.trace_id(),
            remote.span().span_context().trace_id()
        );
        assert_eq!(
            request.parent_span_id,
            remote.span().span_context().span_id()
        );
        assert!(request
            .events
            .events
            .iter()
            .any(|event| event.attributes.iter().any(
                |value| value.key.as_str() == "outcome" && value.value.as_str() == "abandoned"
            )));
        let exported = metric_export.get_finished_metrics().unwrap();
        let debug = format!("{exported:?}");
        assert!(!debug.contains("private-org"));
        assert!(!debug.contains("private-run"));
        assert!(!debug.contains("disallowed"));
        let metric = exported
            .iter()
            .flat_map(|batch| batch.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .find(|metric| metric.name() == "kg_operations")
            .unwrap();
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
            panic!("wrong operation instrument")
        };
        let points: Vec<_> = sum.data_points().collect();
        assert_eq!(points.len(), 1);
        assert!(points[0]
            .attributes()
            .any(|kv| kv.key.as_str() == "outcome" && kv.value.as_str() == "abandoned"));
        assert!(exported
            .iter()
            .flat_map(|batch| batch.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .any(|metric| metric.name() == "kg_queue_wait_ms"));
        let inflight = exported
            .iter()
            .flat_map(|batch| batch.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .find(|metric| metric.name() == "kg_inflight")
            .unwrap();
        let AggregatedMetrics::I64(MetricData::Sum(sum)) = inflight.data() else {
            panic!("wrong inflight instrument");
        };
        assert!(sum.data_points().all(|point| point.value() == 0));
        metrics.shutdown().unwrap();
        traces.shutdown().unwrap();
    }
    #[cfg(feature = "otel")]
    #[tokio::test]
    async fn unavailable_collector_cannot_turn_shutdown_into_ingestion_failure() {
        use opentelemetry::{
            metrics::MeterProvider,
            trace::{Span, Tracer, TracerProvider},
        };
        let config = TelemetryConfig {
            otlp_endpoint: Some("http://127.0.0.1:1".into()),
            export_timeout_ms: 25,
            shutdown_timeout_ms: 100,
            ..Default::default()
        };
        let (traces, metrics) = providers(&config).unwrap();
        traces.tracer("test").start("attempt").end();
        metrics
            .meter("test")
            .u64_counter("export_probe")
            .build()
            .add(1, &[]);
        let guard = TelemetryGuard {
            providers: Some((traces, metrics)),
            shutdown_timeout: Duration::from_millis(100),
        };
        tokio::time::timeout(Duration::from_secs(1), guard.shutdown())
            .await
            .expect("shutdown must be bounded");
    }
}
