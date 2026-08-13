# Distributed Collector — High-Throughput Event-Driven Data Pipeline

**Date:** 2026-08-13  
**Status:** Approved  
**Type:** Portfolio showcase project  

## Purpose

A distributed log processing engine that ingests structured JSON application logs from microservices at ~100K events/sec, validates, enriches, computes rolling aggregates, and stores them in MongoDB. Designed to demonstrate mastery of high concurrency, async event handling, dead-letter queues, batch database writes, and production-grade observability.

## Architecture: Multi-Service Pipeline

Each processing stage is a separate Go service. Events flow through Kafka topics between stages, enabling independent scaling, clear failure boundaries, and visually impressive distributed traces.

```
Producers → [raw-logs] → Validator → [validated-logs] → Enricher → [enriched-logs] → Aggregator → MongoDB (aggregates)
                                                                                    → Raw Store Writer → MongoDB (logs)
```

DLQ topics branch off at each stage for failed events.

## Services

| Service | Responsibility | Kafka Role |
|---------|---------------|------------|
| **Producer/LoadGen** | Generates synthetic structured logs at configurable rate | Producer to `raw-logs` |
| **Validator** | Schema validation, field type checks, rejects malformed events | Consumer of `raw-logs`, Producer to `validated-logs` + `dlq-invalid` |
| **Enricher** | Adds metadata (service registry, timestamp normalization) | Consumer of `validated-logs`, Producer to `enriched-logs` + `dlq-enrich-failed` |
| **Aggregator** | Computes rolling aggregates (error rates, latency percentiles per service per minute) | Consumer of `enriched-logs`, batch writer to MongoDB `aggregates` collection |
| **Raw Store Writer** | Batch-writes enriched logs to MongoDB for retention | Consumer of `enriched-logs`, batch writer to MongoDB `logs` collection |
| **DLQ Replayer** | CLI tool to replay failed events back into the pipeline | Consumer of DLQ topics, producer to origin topics |

## Kafka Topics

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `raw-logs` | 12 | Ingestion (partitioned by service name) |
| `validated-logs` | 12 | Post-validation events |
| `enriched-logs` | 12 | Post-enrichment events |
| `dlq-invalid` | 3 | Schema validation failures |
| `dlq-enrich-failed` | 3 | Enrichment failures |
| `dlq-write-failed` | 3 | MongoDB write failures (after retries) |
| `dlq-poison` | 3 | Events that failed 3+ replay attempts |

## Event Schema

### Input Event (to `raw-logs`)

```json
{
  "timestamp": "2026-08-13T12:30:00.123Z",
  "service": "payment-service",
  "level": "ERROR",
  "message": "failed to process transaction",
  "trace_id": "abc123def456",
  "span_id": "789ghi",
  "parent_span_id": "456def",
  "correlation_id": "corr_tx_98765",
  "metadata": {
    "user_id": "usr_42",
    "endpoint": "/api/v1/charge",
    "method": "POST",
    "latency_ms": 342,
    "status_code": 500,
    "region": "us-east-1",
    "instance_id": "i-0abc123",
    "request_hash": "sha256:e3b0c44298fc...",
    "error_code": "PAYMENT_DECLINED",
    "payload_bytes": 1024,
    "tags": ["critical", "payment", "retry-eligible"]
  }
}
```

### Required Fields (Validator checks)

- `timestamp` — valid ISO 8601
- `service` — non-empty string, max 128 chars
- `level` — one of: DEBUG, INFO, WARN, ERROR, FATAL
- `message` — non-empty string

## Processing Logic

### Validator

- Checks required fields exist and conform to type constraints
- Stateless — scales linearly with replicas
- Invalid events → `dlq-invalid` with error annotation

### Enricher

- Normalizes timestamp to UTC
- Looks up service metadata from in-memory registry (team owner, tier, SLA)
- Adds `processed_at` timestamp
- Computes `hour_bucket` for time-series aggregation
- Tags with pipeline version for schema evolution
- Unknown service → `dlq-enrich-failed`

### Aggregator

- Maintains in-memory tumbling windows (per service, per minute — window resets each minute boundary):
  - Event count by level
  - P50, P95, P99 latency (from `metadata.latency_ms`) using T-Digest approximation
  - Error rate (ERROR+FATAL / total)
  - Top error codes
  - Unique endpoints seen
- Flushes to MongoDB every 10 seconds (configurable)
- Key: `{service}:{minute_timestamp}`
- Upsert writes (idempotent, safe for replays)
- Graceful flush on shutdown

### Raw Store Writer

- Accumulates in buffer, batch-writes with `InsertMany` (ordered: false)
- Flush trigger: 1000 events OR 5 seconds (whichever first)
- Retry: exponential backoff with jitter (100ms → 500ms → 2s), 3 attempts
- After retries exhausted → `dlq-write-failed`
- Computes `expires_at` based on service tier

## Concurrency Model

Each service follows the same internal pattern:

```
Kafka Consumer (1 goroutine/partition)
  → Bounded Channel (capacity configurable, default 64)
    → Worker Pool (N goroutines, default 64)
      → Kafka Async Producer (flush interval 100ms, batch size 500)
```

### Backpressure

1. **Channel-based flow control**: Bounded channels between consumer and workers. Consumer blocks on full channel → pauses Kafka fetching → TCP backpressure to broker.
2. **Consumer lag monitoring**: Exposed as OTel metric. K8s HPA scales replicas when lag exceeds threshold.
3. **Producer backpressure**: Worker blocks on full producer buffer (10K messages max).
4. **Graceful degradation**: MongoDB failures route to DLQ rather than blocking pipeline.

### Graceful Shutdown Sequence

1. Stop consuming (leave consumer group)
2. Drain worker channel
3. Flush producer buffer
4. Flush MongoDB write buffer
5. Close connections, exit

## Dead-Letter Queue Strategy

### DLQ Envelope

```json
{
  "original_event": { },
  "error": {
    "stage": "validator",
    "error_type": "SCHEMA_VIOLATION",
    "error_message": "field 'timestamp' is not valid ISO 8601",
    "failed_at": "2026-08-13T12:30:01.456Z",
    "retry_count": 0,
    "retry_history": [],
    "poison_pill": false,
    "service_instance": "validator-pod-abc123"
  }
}
```

### Error Classification

```go
type ErrorClass int
const (
    Permanent ErrorClass = iota  // schema violations — no retry, immediate DLQ
    Transient                     // timeouts, connection issues — retry with exponential backoff
    RateLimited                   // upstream/downstream rate limit hit — retry with longer, fixed-interval backoff (5s)
)
```

### Retry Strategy

- **Permanent failures**: No retry, straight to DLQ.
- **Transient failures**: Exponential backoff with jitter. 3 attempts (100ms → 500ms → 2s). Then DLQ.
- **DLQ replay**: `dlq-replayer` CLI republishes events to origin topic, incrementing `retry_count`.

### Poison-Pill Detection

- Events with `retry_count >= 3` are classified as poison pills
- Routed directly to `dlq-poison` topic (never retried again)
- Full `retry_history` preserved for debugging audit trail

## MongoDB Schema

### Collection: `logs`

```json
{
  "_id": "ObjectId",
  "timestamp": "ISODate",
  "service": "payment-service",
  "level": "ERROR",
  "message": "...",
  "trace_id": "...",
  "correlation_id": "...",
  "metadata": {},
  "enrichment": {
    "team": "payments",
    "tier": "critical",
    "processed_at": "ISODate",
    "hour_bucket": "ISODate",
    "pipeline_version": "1.0.0"
  },
  "expires_at": "ISODate",
  "retention_tier": "critical",
  "ingested_at": "ISODate"
}
```

**Indexes:**
- `{ service: 1, timestamp: -1 }` — logs by service
- `{ level: 1, timestamp: -1 }` — errors by time
- `{ trace_id: 1 }` — trace correlation
- `{ correlation_id: 1 }` — cross-service grouping
- `{ expires_at: 1 }` with `expireAfterSeconds: 0` — TTL auto-deletion

### TTL by Service Tier

| Tier | Retention |
|------|-----------|
| `critical` | 90 days |
| `standard` | 30 days |
| `debug` | 7 days |

### Collection: `aggregates`

```json
{
  "_id": "ObjectId",
  "service": "payment-service",
  "bucket": "ISODate (minute granularity)",
  "window_seconds": 60,
  "counts": { "total": 1523, "debug": 200, "info": 1000, "warn": 200, "error": 100, "fatal": 23 },
  "latency": { "p50_ms": 45, "p95_ms": 230, "p99_ms": 890 },
  "error_rate": 0.0807,
  "unique_endpoints": ["/api/v1/charge", "/api/v1/refund"],
  "top_errors": [{"code": "PAYMENT_DECLINED", "count": 55}],
  "computed_at": "ISODate"
}
```

**Indexes:**
- `{ service: 1, bucket: -1 }` unique compound — time-series per service
- `{ bucket: -1 }` — global time range
- `{ error_rate: -1, bucket: -1 }` — highest error rates

**Write strategy:**
- Raw Store Writer: `InsertMany`, `ordered: false`, batch 1000
- Aggregator: `UpdateOne` with `upsert: true` on `{ service, bucket }`

## Observability

### Distributed Tracing (OpenTelemetry + Jaeger)

Trace context propagated via Kafka message headers (W3C Trace Context). Each service creates a span:

```
Producer (create trace) → Validator (span) → Enricher (span) → Aggregator (span)
                                                              → Raw Store Writer (span)
```

### Metrics (OTel → Prometheus endpoint)

| Metric | Type | Labels |
|--------|------|--------|
| `pipeline_events_processed_total` | Counter | `stage`, `service`, `status` |
| `pipeline_event_latency_seconds` | Histogram | `stage` |
| `pipeline_batch_size` | Histogram | `stage`, `target` |
| `pipeline_consumer_lag` | Gauge | `stage`, `partition` |
| `pipeline_dlq_events_total` | Counter | `stage`, `error_type` |
| `pipeline_mongo_write_duration_seconds` | Histogram | `collection`, `operation` |
| `pipeline_kafka_produce_duration_seconds` | Histogram | `topic` |
| `pipeline_worker_pool_utilization` | Gauge | `stage` |

### Infrastructure

- Jaeger all-in-one (local) / distributed (K8s)
- OTel Collector sidecar
- Pre-built Grafana dashboard JSON (pipeline health overview)
- Prometheus for metric storage/queries

### Structured Logging

Go `slog` with JSON output. Every log line includes `trace_id` for log-trace correlation.

## Load Generator

### CLI: `cmd/loadgen`

**Modes:**

| Mode | Behavior |
|------|----------|
| `steady` | Constant rate indefinitely |
| `burst` | Alternates low (10K/s) and high (150K/s) every 30s |
| `ramp` | Linear increase from 0 to target over N seconds |
| `chaos` | Random services, error rates, payload sizes |
| `vegeta` | HTTP load via vegeta against ingestion endpoint |

**Usage:**

```bash
./loadgen --mode=steady --rate=100000 --kafka-brokers=localhost:9092 --topic=raw-logs
./loadgen --mode=ramp --target-rate=100000 --ramp-seconds=60 --http-endpoint=http://localhost:8080/ingest
./loadgen --mode=vegeta --rate=50000 --duration=30s --http-endpoint=http://localhost:8080/ingest
```

**Synthetic data characteristics:**
- 20 configurable fake services
- Log level distribution: 70% INFO, 15% WARN, 10% ERROR, 4% DEBUG, 1% FATAL
- Latency values: log-normal distribution (realistic tail)
- ~2% intentionally invalid events (DLQ testing)

### Benchmark Reports

Output to `./benchmarks/report-{timestamp}.json` and `./benchmarks/report-{timestamp}.md`.

Markdown report includes: achieved throughput, end-to-end latency percentiles, DLQ rates, consumer lag per stage, system resource usage.

### HTTP Ingestion Endpoint

Optional `POST /ingest` endpoint (202 Accepted) for services that can't speak Kafka directly. Used by vegeta mode.

## Deployment

### Docker Compose (Local)

Single `docker-compose up` brings up: Kafka (3 brokers), MongoDB, Jaeger, OTel Collector, Grafana, Prometheus, all pipeline services (with replicas), load generator.

### Kubernetes

```
deploy/k8s/
├── namespace.yaml
├── kafka/strimzi-kafka-cluster.yaml
├── mongodb/mongodb-statefulset.yaml (3-node replica set)
├── observability/ (jaeger, otel-collector, grafana, prometheus)
├── pipeline/ (deployments + HPA for each service, configmap)
└── load-test/load-generator-job.yaml
```

**HPA configuration:**
- Trigger: Kafka consumer lag (via KEDA or Prometheus adapter custom metric)
- Min replicas: 2, Max: 10 per service
- Scale-up threshold: lag > 10,000 messages for 30 seconds

### Build

- Multi-stage Dockerfiles (builder + distroless)
- Makefile targets: `build`, `test`, `lint`, `bench`, `docker-build`, `docker-push`, `up` (compose), `k8s-deploy`

## Repository Structure

```
distributed-collector/
├── cmd/
│   ├── validator/main.go
│   ├── enricher/main.go
│   ├── aggregator/main.go
│   ├── raw-store-writer/main.go
│   ├── loadgen/main.go
│   └── dlq-replayer/main.go
├── internal/
│   ├── kafka/          # Shared consumer/producer wrappers
│   ├── mongo/          # Client, batch writer, connection pool
│   ├── telemetry/      # OTel setup, span helpers, metrics
│   ├── config/         # Env-based config loading
│   ├── model/          # Event structs, DLQ envelope, aggregates
│   ├── dlq/            # DLQ routing, poison-pill detection
│   ├── backpressure/   # Bounded channels, worker pool
│   └── registry/       # Service registry (in-memory)
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile.{service}
│   │   └── docker-compose.yaml
│   └── k8s/
├── configs/
│   ├── service-registry.yaml
│   └── pipeline.yaml
├── dashboards/
│   └── grafana-pipeline.json
├── benchmarks/
├── docs/
│   └── architecture.md
├── Makefile
├── go.mod
├── go.sum
└── README.md
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.22+ |
| Messaging | Apache Kafka (Sarama client) |
| Database | MongoDB (mongo-driver) |
| Tracing | OpenTelemetry SDK + Jaeger |
| Metrics | OpenTelemetry → Prometheus |
| Dashboards | Grafana |
| Containers | Docker (multi-stage, distroless) |
| Orchestration | Kubernetes + Strimzi (Kafka) + KEDA (autoscaling) |
| Load Testing | Custom Go CLI + vegeta |
| Linting | golangci-lint |
| Logging | Go slog (structured JSON) |

## Success Criteria

1. Sustain 100K events/sec through the full pipeline on a single machine (Docker Compose)
2. End-to-end latency P95 < 100ms (from Kafka produce to MongoDB write)
3. Zero data loss under normal operation (all events either stored or in DLQ)
4. Graceful handling of MongoDB outage (events route to DLQ, pipeline doesn't crash)
5. Horizontal scaling demonstrated via K8s HPA responding to consumer lag
6. Full distributed trace visible in Jaeger for any event's journey
7. Grafana dashboard shows real-time pipeline health
8. README enables a reviewer to run the full system in under 3 minutes
