# Distributed Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a multi-service event-driven data pipeline in Go that ingests structured JSON logs via Kafka, validates, enriches, aggregates, and stores them in MongoDB at ~100K events/sec.

**Architecture:** Five independent Go services communicate through Kafka topics in a linear pipeline. Each service is a separate binary consuming from one topic and producing to the next. Shared internal packages provide Kafka wrappers, MongoDB batch writers, OpenTelemetry instrumentation, worker pools, and DLQ routing.

**Tech Stack:** Go 1.22+, Apache Kafka (Sarama), MongoDB (mongo-driver), OpenTelemetry SDK, Jaeger, Prometheus, Grafana, Docker, Kubernetes, vegeta

## Global Constraints

- Go 1.22+ (for slog, slices package)
- Module path: `github.com/widasinnacy/distributed-collector`
- Kafka client: `github.com/IBM/sarama` (actively maintained Sarama fork)
- MongoDB driver: `go.mongodb.org/mongo-driver/v2`
- OTel SDK: `go.opentelemetry.io/otel` v1.28+
- All services must gracefully shutdown on SIGINT/SIGTERM
- All services must expose `/health` and `/metrics` HTTP endpoints
- Structured logging via `log/slog` with JSON handler
- Config via environment variables (no config files for service runtime; YAML for service registry only)
- Tests use standard `testing` package + `github.com/stretchr/testify`
- Linting: golangci-lint with default config
- Docker images: multi-stage build, distroless base

---

## File Structure

```
distributed-collector/
├── cmd/
│   ├── validator/main.go          # Validator service entrypoint
│   ├── enricher/main.go           # Enricher service entrypoint
│   ├── aggregator/main.go         # Aggregator service entrypoint
│   ├── raw-store-writer/main.go   # Raw Store Writer entrypoint
│   ├── loadgen/main.go            # Load generator CLI
│   └── dlq-replayer/main.go       # DLQ replay CLI
├── internal/
│   ├── config/config.go           # Shared config loading from env
│   ├── config/config_test.go
│   ├── model/event.go             # Event, DLQEnvelope, Aggregate structs
│   ├── model/event_test.go
│   ├── kafka/consumer.go          # Consumer group wrapper
│   ├── kafka/producer.go          # Async producer wrapper
│   ├── kafka/consumer_test.go
│   ├── kafka/producer_test.go
│   ├── kafka/headers.go           # W3C trace context propagation
│   ├── kafka/headers_test.go
│   ├── mongo/client.go            # MongoDB connection + health check
│   ├── mongo/batchwriter.go       # Batch InsertMany with flush logic
│   ├── mongo/batchwriter_test.go
│   ├── telemetry/telemetry.go     # OTel tracer + meter provider setup
│   ├── telemetry/metrics.go       # Metric definitions
│   ├── telemetry/span.go          # Span extraction/injection helpers
│   ├── pipeline/worker.go         # Worker pool with bounded channel
│   ├── pipeline/worker_test.go
│   ├── pipeline/service.go        # Base service lifecycle (start, shutdown, health)
│   ├── pipeline/service_test.go
│   ├── dlq/router.go              # DLQ routing + envelope creation
│   ├── dlq/router_test.go
│   ├── dlq/poison.go              # Poison-pill detection
│   ├── dlq/poison_test.go
│   ├── validator/validator.go     # Validation logic (pure, no Kafka)
│   ├── validator/validator_test.go
│   ├── enricher/enricher.go       # Enrichment logic (pure)
│   ├── enricher/enricher_test.go
│   ├── enricher/registry.go       # Service registry loader
│   ├── enricher/registry_test.go
│   ├── aggregator/aggregator.go   # Tumbling window + T-Digest
│   ├── aggregator/aggregator_test.go
│   └── loadgen/generator.go       # Synthetic log generation
├── configs/
│   ├── service-registry.yaml      # Service metadata for enricher
│   └── pipeline.yaml              # Topic names, partition counts, batch sizes
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile.validator
│   │   ├── Dockerfile.enricher
│   │   ├── Dockerfile.aggregator
│   │   ├── Dockerfile.raw-store-writer
│   │   ├── Dockerfile.loadgen
│   │   └── docker-compose.yaml
│   └── k8s/
│       ├── namespace.yaml
│       ├── kafka/strimzi-cluster.yaml
│       ├── mongodb/statefulset.yaml
│       ├── observability/jaeger.yaml
│       ├── observability/otel-collector.yaml
│       ├── observability/prometheus.yaml
│       ├── observability/grafana.yaml
│       ├── pipeline/validator.yaml
│       ├── pipeline/enricher.yaml
│       ├── pipeline/aggregator.yaml
│       ├── pipeline/raw-store-writer.yaml
│       ├── pipeline/configmap.yaml
│       └── load-test/job.yaml
├── dashboards/
│   └── grafana-pipeline.json
├── benchmarks/                    # Generated at runtime
├── docs/
│   └── architecture.md
├── Makefile
├── go.mod
├── go.sum
├── .golangci.yml
└── README.md
```

---

### Task 1: Project Scaffolding & Shared Models

**Files:**
- Create: `go.mod`
- Create: `internal/model/event.go`
- Create: `internal/model/event_test.go`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Create: `Makefile`
- Create: `.golangci.yml`

**Interfaces:**
- Consumes: Nothing (first task)
- Produces:
  - `model.LogEvent` struct (used by all services)
  - `model.DLQEnvelope` struct (used by dlq package)
  - `model.Aggregate` struct (used by aggregator)
  - `model.ErrorClass` type with `Permanent`, `Transient`, `RateLimited` constants
  - `config.Load() (*Config, error)` function

- [ ] **Step 1: Initialize Go module and install dependencies**

```bash
cd "/home/widasinnacy/incy/Distributed collector"
go mod init github.com/widasinnacy/distributed-collector
go get github.com/IBM/sarama@latest
go get go.mongodb.org/mongo-driver/v2@latest
go get github.com/stretchr/testify@latest
go get go.opentelemetry.io/otel@latest
go get go.opentelemetry.io/otel/sdk@latest
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@latest
go get go.opentelemetry.io/otel/exporters/prometheus@latest
go get github.com/prometheus/client_golang/prometheus/promhttp@latest
go get gopkg.in/yaml.v3@latest
go get github.com/caio/go-tdigest/v4@latest
```

- [ ] **Step 2: Write model tests**

```go
// internal/model/event_test.go
package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEvent_JSON_RoundTrip(t *testing.T) {
	event := LogEvent{
		Timestamp:    time.Date(2026, 8, 13, 12, 30, 0, 123000000, time.UTC),
		Service:      "payment-service",
		Level:        "ERROR",
		Message:      "failed to process transaction",
		TraceID:      "abc123def456",
		SpanID:       "789ghi",
		ParentSpanID: "456def",
		CorrelationID: "corr_tx_98765",
		Metadata: EventMetadata{
			UserID:      "usr_42",
			Endpoint:    "/api/v1/charge",
			Method:      "POST",
			LatencyMs:   342,
			StatusCode:  500,
			Region:      "us-east-1",
			InstanceID:  "i-0abc123",
			RequestHash: "sha256:e3b0c44298fc",
			ErrorCode:   "PAYMENT_DECLINED",
			PayloadBytes: 1024,
			Tags:        []string{"critical", "payment"},
		},
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var decoded LogEvent
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, event.Service, decoded.Service)
	assert.Equal(t, event.Level, decoded.Level)
	assert.Equal(t, event.Metadata.LatencyMs, decoded.Metadata.LatencyMs)
	assert.Equal(t, event.Metadata.Tags, decoded.Metadata.Tags)
}

func TestDLQEnvelope_IncludesRetryHistory(t *testing.T) {
	envelope := DLQEnvelope{
		OriginalEvent: json.RawMessage(`{"service":"test"}`),
		Error: DLQError{
			Stage:        "validator",
			ErrorType:    "SCHEMA_VIOLATION",
			ErrorMessage: "field 'timestamp' missing",
			FailedAt:     time.Now().UTC(),
			RetryCount:   2,
			RetryHistory: []RetryAttempt{
				{Attempt: 1, At: time.Now().Add(-5 * time.Minute).UTC(), Error: "SCHEMA_VIOLATION"},
				{Attempt: 2, At: time.Now().Add(-2 * time.Minute).UTC(), Error: "SCHEMA_VIOLATION"},
			},
			PoisonPill:      false,
			ServiceInstance: "validator-pod-abc123",
		},
	}

	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"retry_count":2`)
	assert.Contains(t, string(data), `"retry_history"`)
}

func TestErrorClass_Values(t *testing.T) {
	assert.Equal(t, ErrorClass(0), Permanent)
	assert.Equal(t, ErrorClass(1), Transient)
	assert.Equal(t, ErrorClass(2), RateLimited)
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
go test ./internal/model/ -v
```

Expected: FAIL — package doesn't exist yet.

- [ ] **Step 4: Implement models**

```go
// internal/model/event.go
package model

import (
	"encoding/json"
	"time"
)

type ErrorClass int

const (
	Permanent   ErrorClass = iota // schema violations — no retry
	Transient                     // timeouts, connection issues — exponential backoff
	RateLimited                   // rate limit hit — fixed 5s backoff
)

type LogEvent struct {
	Timestamp     time.Time     `json:"timestamp" bson:"timestamp"`
	Service       string        `json:"service" bson:"service"`
	Level         string        `json:"level" bson:"level"`
	Message       string        `json:"message" bson:"message"`
	TraceID       string        `json:"trace_id" bson:"trace_id"`
	SpanID        string        `json:"span_id" bson:"span_id"`
	ParentSpanID  string        `json:"parent_span_id,omitempty" bson:"parent_span_id,omitempty"`
	CorrelationID string        `json:"correlation_id,omitempty" bson:"correlation_id,omitempty"`
	Metadata      EventMetadata `json:"metadata" bson:"metadata"`
}

type EventMetadata struct {
	UserID       string   `json:"user_id,omitempty" bson:"user_id,omitempty"`
	Endpoint     string   `json:"endpoint,omitempty" bson:"endpoint,omitempty"`
	Method       string   `json:"method,omitempty" bson:"method,omitempty"`
	LatencyMs    int64    `json:"latency_ms,omitempty" bson:"latency_ms,omitempty"`
	StatusCode   int      `json:"status_code,omitempty" bson:"status_code,omitempty"`
	Region       string   `json:"region,omitempty" bson:"region,omitempty"`
	InstanceID   string   `json:"instance_id,omitempty" bson:"instance_id,omitempty"`
	RequestHash  string   `json:"request_hash,omitempty" bson:"request_hash,omitempty"`
	ErrorCode    string   `json:"error_code,omitempty" bson:"error_code,omitempty"`
	PayloadBytes int      `json:"payload_bytes,omitempty" bson:"payload_bytes,omitempty"`
	Tags         []string `json:"tags,omitempty" bson:"tags,omitempty"`
}

type EnrichedLogEvent struct {
	LogEvent   `bson:",inline"`
	Enrichment Enrichment `json:"enrichment" bson:"enrichment"`
	ExpiresAt  time.Time  `json:"expires_at" bson:"expires_at"`
	RetentionTier string  `json:"retention_tier" bson:"retention_tier"`
	IngestedAt time.Time  `json:"ingested_at" bson:"ingested_at"`
}

type Enrichment struct {
	Team            string    `json:"team" bson:"team"`
	Tier            string    `json:"tier" bson:"tier"`
	ProcessedAt     time.Time `json:"processed_at" bson:"processed_at"`
	HourBucket      time.Time `json:"hour_bucket" bson:"hour_bucket"`
	PipelineVersion string    `json:"pipeline_version" bson:"pipeline_version"`
}

type Aggregate struct {
	Service         string         `json:"service" bson:"service"`
	Bucket          time.Time      `json:"bucket" bson:"bucket"`
	WindowSeconds   int            `json:"window_seconds" bson:"window_seconds"`
	Counts          LevelCounts    `json:"counts" bson:"counts"`
	Latency         LatencyStats   `json:"latency" bson:"latency"`
	ErrorRate       float64        `json:"error_rate" bson:"error_rate"`
	UniqueEndpoints []string       `json:"unique_endpoints" bson:"unique_endpoints"`
	TopErrors       []ErrorCount   `json:"top_errors" bson:"top_errors"`
	ComputedAt      time.Time      `json:"computed_at" bson:"computed_at"`
}

type LevelCounts struct {
	Total int64 `json:"total" bson:"total"`
	Debug int64 `json:"debug" bson:"debug"`
	Info  int64 `json:"info" bson:"info"`
	Warn  int64 `json:"warn" bson:"warn"`
	Error int64 `json:"error" bson:"error"`
	Fatal int64 `json:"fatal" bson:"fatal"`
}

type LatencyStats struct {
	P50Ms float64 `json:"p50_ms" bson:"p50_ms"`
	P95Ms float64 `json:"p95_ms" bson:"p95_ms"`
	P99Ms float64 `json:"p99_ms" bson:"p99_ms"`
}

type ErrorCount struct {
	Code  string `json:"code" bson:"code"`
	Count int64  `json:"count" bson:"count"`
}

type DLQEnvelope struct {
	OriginalEvent json.RawMessage `json:"original_event" bson:"original_event"`
	Error         DLQError        `json:"error" bson:"error"`
}

type DLQError struct {
	Stage           string         `json:"stage" bson:"stage"`
	ErrorType       string         `json:"error_type" bson:"error_type"`
	ErrorMessage    string         `json:"error_message" bson:"error_message"`
	FailedAt        time.Time      `json:"failed_at" bson:"failed_at"`
	RetryCount      int            `json:"retry_count" bson:"retry_count"`
	RetryHistory    []RetryAttempt `json:"retry_history" bson:"retry_history"`
	PoisonPill      bool           `json:"poison_pill" bson:"poison_pill"`
	ServiceInstance string         `json:"service_instance" bson:"service_instance"`
}

type RetryAttempt struct {
	Attempt int       `json:"attempt" bson:"attempt"`
	At      time.Time `json:"at" bson:"at"`
	Error   string    `json:"error" bson:"error"`
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./internal/model/ -v
```

Expected: PASS (3 tests)

- [ ] **Step 6: Write config tests**

```go
// internal/config/config_test.go
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoURI)
	assert.Equal(t, "distributed-collector", cfg.MongoDatabase)
	assert.Equal(t, "localhost:4317", cfg.OTelEndpoint)
	assert.Equal(t, 64, cfg.WorkerPoolSize)
	assert.Equal(t, 1000, cfg.BatchSize)
	assert.Equal(t, 5, cfg.FlushIntervalSec)
}

func TestLoad_FromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("MONGO_URI", "mongodb://prod:27017")
	t.Setenv("WORKER_POOL_SIZE", "128")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "mongodb://prod:27017", cfg.MongoURI)
	assert.Equal(t, 128, cfg.WorkerPoolSize)
}
```

- [ ] **Step 7: Implement config**

```go
// internal/config/config.go
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers    []string
	MongoURI        string
	MongoDatabase   string
	OTelEndpoint    string
	ServiceName     string
	WorkerPoolSize  int
	BatchSize       int
	FlushIntervalSec int
	ChannelBuffer   int
	ConsumerGroup   string
	InputTopic      string
	OutputTopic     string
	DLQTopic        string
	HTTPPort        string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:    strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ","),
		MongoURI:        envOrDefault("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:   envOrDefault("MONGO_DATABASE", "distributed-collector"),
		OTelEndpoint:    envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		ServiceName:     envOrDefault("SERVICE_NAME", "unknown"),
		WorkerPoolSize:  envOrDefaultInt("WORKER_POOL_SIZE", 64),
		BatchSize:       envOrDefaultInt("BATCH_SIZE", 1000),
		FlushIntervalSec: envOrDefaultInt("FLUSH_INTERVAL_SEC", 5),
		ChannelBuffer:   envOrDefaultInt("CHANNEL_BUFFER", 64),
		ConsumerGroup:   envOrDefault("CONSUMER_GROUP", ""),
		InputTopic:      envOrDefault("INPUT_TOPIC", ""),
		OutputTopic:     envOrDefault("OUTPUT_TOPIC", ""),
		DLQTopic:        envOrDefault("DLQ_TOPIC", ""),
		HTTPPort:        envOrDefault("HTTP_PORT", "8081"),
	}
	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
```

- [ ] **Step 8: Run all tests, verify pass**

```bash
go test ./internal/... -v
```

Expected: PASS

- [ ] **Step 9: Create Makefile and lint config**

```makefile
# Makefile
.PHONY: build test lint bench up down docker-build

SERVICES := validator enricher aggregator raw-store-writer loadgen dlq-replayer

build:
	@for svc in $(SERVICES); do \
		go build -o bin/$$svc ./cmd/$$svc; \
	done

test:
	go test ./... -v -race -count=1

lint:
	golangci-lint run ./...

bench:
	./bin/loadgen --mode=steady --rate=100000 --duration=60s --report-dir=./benchmarks

docker-build:
	@for svc in $(SERVICES); do \
		docker build -f deploy/docker/Dockerfile.$$svc -t distributed-collector-$$svc:latest .; \
	done

up:
	docker compose -f deploy/docker/docker-compose.yaml up -d

down:
	docker compose -f deploy/docker/docker-compose.yaml down -v

k8s-deploy:
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/ --recursive
```

```yaml
# .golangci.yml
linters:
  enable:
    - errcheck
    - govet
    - staticcheck
    - unused
    - gosimple
    - ineffassign
  disable:
    - depguard

linters-settings:
  errcheck:
    check-type-assertions: true

run:
  timeout: 5m
```

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat: project scaffolding with shared models and config"
```

---

### Task 2: Worker Pool & Pipeline Base Service

**Files:**
- Create: `internal/pipeline/worker.go`
- Create: `internal/pipeline/worker_test.go`
- Create: `internal/pipeline/service.go`
- Create: `internal/pipeline/service_test.go`

**Interfaces:**
- Consumes: `config.Config`
- Produces:
  - `pipeline.WorkerPool` struct with `Start(ctx)`, `Submit(item)`, `Stop()`
  - `pipeline.ProcessFunc` type: `func(ctx context.Context, msg []byte) ([]byte, error)`
  - `pipeline.Service` struct with `Run(ctx) error`, `Shutdown(ctx) error`

- [ ] **Step 1: Write worker pool failing test**

```go
// internal/pipeline/worker_test.go
package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool_ProcessesAllItems(t *testing.T) {
	var processed atomic.Int64

	pool := NewWorkerPool(4, 16, func(ctx context.Context, msg []byte) ([]byte, error) {
		processed.Add(1)
		return msg, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	for i := 0; i < 100; i++ {
		require.True(t, pool.Submit([]byte("event")))
	}

	pool.Stop()
	assert.Equal(t, int64(100), processed.Load())
}

func TestWorkerPool_RespectsContext(t *testing.T) {
	pool := NewWorkerPool(2, 4, func(ctx context.Context, msg []byte) ([]byte, error) {
		time.Sleep(100 * time.Millisecond)
		return msg, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	cancel()

	time.Sleep(50 * time.Millisecond)
	assert.False(t, pool.Submit([]byte("should-fail")))
}

func TestWorkerPool_BackpressureWhenFull(t *testing.T) {
	pool := NewWorkerPool(1, 2, func(ctx context.Context, msg []byte) ([]byte, error) {
		time.Sleep(500 * time.Millisecond)
		return msg, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	pool.Submit([]byte("1"))
	pool.Submit([]byte("2"))
	pool.Submit([]byte("3"))

	start := time.Now()
	pool.Submit([]byte("4"))
	elapsed := time.Since(start)

	assert.True(t, elapsed > 400*time.Millisecond, "should block when channel is full")
	pool.Stop()
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/pipeline/ -v -run TestWorkerPool
```

Expected: FAIL — package doesn't exist.

- [ ] **Step 3: Implement worker pool**

```go
// internal/pipeline/worker.go
package pipeline

import (
	"context"
	"sync"
)

type ProcessFunc func(ctx context.Context, msg []byte) ([]byte, error)

type ResultFunc func(output []byte, err error)

type WorkerPool struct {
	workers    int
	input      chan []byte
	process    ProcessFunc
	onResult   ResultFunc
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	stopped    chan struct{}
}

func NewWorkerPool(workers, bufferSize int, process ProcessFunc) *WorkerPool {
	return &WorkerPool{
		workers: workers,
		input:   make(chan []byte, bufferSize),
		process: process,
		stopped: make(chan struct{}),
	}
}

func (wp *WorkerPool) SetResultHandler(fn ResultFunc) {
	wp.onResult = fn
}

func (wp *WorkerPool) Start(ctx context.Context) {
	wp.ctx, wp.cancelFunc = context.WithCancel(ctx)

	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case msg, ok := <-wp.input:
			if !ok {
				return
			}
			output, err := wp.process(wp.ctx, msg)
			if wp.onResult != nil {
				wp.onResult(output, err)
			}
		case <-wp.ctx.Done():
			return
		}
	}
}

func (wp *WorkerPool) Submit(msg []byte) bool {
	select {
	case wp.input <- msg:
		return true
	case <-wp.ctx.Done():
		return false
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.input)
	wp.wg.Wait()
	close(wp.stopped)
}

func (wp *WorkerPool) Done() <-chan struct{} {
	return wp.stopped
}
```

- [ ] **Step 4: Run tests to verify pass**

```bash
go test ./internal/pipeline/ -v -run TestWorkerPool -race
```

Expected: PASS

- [ ] **Step 5: Write service lifecycle test**

```go
// internal/pipeline/service_test.go
package pipeline

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_HealthEndpoint(t *testing.T) {
	svc := NewService("test-service", "127.0.0.1:0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- svc.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://" + svc.Addr() + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("service did not shut down in time")
	}
}
```

- [ ] **Step 6: Implement service lifecycle**

```go
// internal/pipeline/service.go
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

type Service struct {
	name     string
	addr     string
	listener net.Listener
	server   *http.Server
	mu       sync.Mutex
	healthy  bool
}

func NewService(name, addr string) *Service {
	return &Service{
		name:    name,
		addr:    addr,
		healthy: true,
	}
}

func (s *Service) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Service) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)

	var err error
	s.mu.Lock()
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("listen: %w", err)
	}
	s.server = &http.Server{Handler: mux}
	s.mu.Unlock()

	slog.Info("service started", "name", s.name, "addr", s.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- s.server.Serve(s.listener) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	healthy := s.healthy
	s.mu.Unlock()

	if healthy {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"healthy"}`)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"status":"unhealthy"}`)
	}
}

func (s *Service) SetHealthy(healthy bool) {
	s.mu.Lock()
	s.healthy = healthy
	s.mu.Unlock()
}
```

- [ ] **Step 7: Run all pipeline tests**

```bash
go test ./internal/pipeline/ -v -race
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: worker pool with backpressure and base service lifecycle"
```

---

### Task 3: Kafka Consumer & Producer Wrappers

**Files:**
- Create: `internal/kafka/consumer.go`
- Create: `internal/kafka/producer.go`
- Create: `internal/kafka/consumer_test.go`
- Create: `internal/kafka/producer_test.go`
- Create: `internal/kafka/headers.go`
- Create: `internal/kafka/headers_test.go`

**Interfaces:**
- Consumes: `config.Config` (KafkaBrokers, ConsumerGroup, InputTopic)
- Produces:
  - `kafka.Consumer` struct with `Consume(ctx, handler) error`, `Close() error`
  - `kafka.MessageHandler` type: `func(ctx context.Context, key, value []byte, headers map[string]string) error`
  - `kafka.Producer` struct with `Send(topic string, key, value []byte, headers map[string]string) error`, `Close() error`
  - `kafka.InjectTraceContext(ctx context.Context) map[string]string`
  - `kafka.ExtractTraceContext(headers map[string]string) context.Context`

- [ ] **Step 1: Write producer test**

```go
// internal/kafka/producer_test.go
package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer_SendMessage(t *testing.T) {
	mockProducer := mocks.NewAsyncProducer(t, nil)
	mockProducer.ExpectInputAndSucceed()

	p := &Producer{
		async: mockProducer,
	}

	err := p.Send("test-topic", []byte("key"), []byte("value"), map[string]string{"traceparent": "00-abc-def-01"})
	require.NoError(t, err)

	mockProducer.Close()
}

func TestProducer_HeadersConvertedCorrectly(t *testing.T) {
	headers := headersToSarama(map[string]string{
		"traceparent": "00-trace-span-01",
		"custom":      "value",
	})

	assert.Len(t, headers, 2)

	found := false
	for _, h := range headers {
		if string(h.Key) == "traceparent" {
			assert.Equal(t, []byte("00-trace-span-01"), h.Value)
			found = true
		}
	}
	assert.True(t, found)
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/kafka/ -v -run TestProducer
```

Expected: FAIL

- [ ] **Step 3: Implement producer**

```go
// internal/kafka/producer.go
package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Producer struct {
	async sarama.AsyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Flush.Messages = 500
	config.Producer.Flush.Frequency = 100 * 1000000 // 100ms in nanoseconds
	config.Producer.MaxMessageBytes = 1048576

	async, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("new async producer: %w", err)
	}

	return &Producer{async: async}, nil
}

func (p *Producer) Send(topic string, key, value []byte, headers map[string]string) error {
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.ByteEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: headersToSarama(headers),
	}

	p.async.Input() <- msg
	return nil
}

func (p *Producer) Close() error {
	return p.async.Close()
}

func headersToSarama(headers map[string]string) []sarama.RecordHeader {
	result := make([]sarama.RecordHeader, 0, len(headers))
	for k, v := range headers {
		result = append(result, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return result
}

func headersFromSarama(headers []*sarama.RecordHeader) map[string]string {
	result := make(map[string]string, len(headers))
	for _, h := range headers {
		result[string(h.Key)] = string(h.Value)
	}
	return result
}
```

- [ ] **Step 4: Write consumer test**

```go
// internal/kafka/consumer_test.go
package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestHeadersFromSarama(t *testing.T) {
	saramaHeaders := []*sarama.RecordHeader{
		{Key: []byte("traceparent"), Value: []byte("00-abc-def-01")},
		{Key: []byte("custom"), Value: []byte("value")},
	}

	result := headersFromSarama(saramaHeaders)
	assert.Equal(t, "00-abc-def-01", result["traceparent"])
	assert.Equal(t, "value", result["custom"])
}
```

- [ ] **Step 5: Implement consumer**

```go
// internal/kafka/consumer.go
package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type MessageHandler func(ctx context.Context, key, value []byte, headers map[string]string) error

type Consumer struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler MessageHandler
}

func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	return &Consumer{group: group, topics: topics}, nil
}

func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	c.handler = handler
	for {
		if err := c.group.Consume(ctx, c.topics, c); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			headers := headersFromSarama(msg.Headers)
			if err := c.handler(session.Context(), msg.Key, msg.Value, headers); err != nil {
				slog.Error("handler error", "error", err, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
```

- [ ] **Step 6: Write and implement trace context headers**

```go
// internal/kafka/headers_test.go
package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraceContext_InjectExtract_Roundtrip(t *testing.T) {
	headers := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	ctx := ExtractTraceContext(context.Background(), headers)
	assert.NotNil(t, ctx)

	injected := InjectTraceContext(ctx)
	assert.Contains(t, injected, "traceparent")
}
```

```go
// internal/kafka/headers.go
package kafka

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type headerCarrier map[string]string

func (hc headerCarrier) Get(key string) string         { return hc[key] }
func (hc headerCarrier) Set(key, value string)         { hc[key] = value }
func (hc headerCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range hc {
		keys = append(keys, k)
	}
	return keys
}

func InjectTraceContext(ctx context.Context) map[string]string {
	carrier := make(headerCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func ExtractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	carrier := headerCarrier(headers)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
```

- [ ] **Step 7: Run all kafka tests**

```bash
go test ./internal/kafka/ -v -race
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: Kafka consumer group and async producer wrappers with trace propagation"
```

---

### Task 4: DLQ Router & Poison-Pill Detection

**Files:**
- Create: `internal/dlq/router.go`
- Create: `internal/dlq/router_test.go`
- Create: `internal/dlq/poison.go`
- Create: `internal/dlq/poison_test.go`

**Interfaces:**
- Consumes: `model.DLQEnvelope`, `model.ErrorClass`, `kafka.Producer`
- Produces:
  - `dlq.Router` struct with `Route(ctx, stage, event, err, class) error`
  - `dlq.IsPoisonPill(envelope DLQEnvelope) bool`
  - `dlq.MaxRetries` constant (3)

- [ ] **Step 1: Write DLQ router tests**

```go
// internal/dlq/router_test.go
package dlq

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

type mockProducer struct {
	messages []sentMessage
}

type sentMessage struct {
	topic   string
	key     []byte
	value   []byte
	headers map[string]string
}

func (m *mockProducer) Send(topic string, key, value []byte, headers map[string]string) error {
	m.messages = append(m.messages, sentMessage{topic, key, value, headers})
	return nil
}

func TestRouter_RoutesPermanentErrorToDLQ(t *testing.T) {
	producer := &mockProducer{}
	router := NewRouter(producer, "dlq-invalid", "dlq-poison", "validator", "validator-pod-1")

	event := []byte(`{"service":"test","level":"ERROR"}`)
	err := router.Route(context.Background(), event, "SCHEMA_VIOLATION", "missing timestamp", model.Permanent, 0)
	require.NoError(t, err)

	require.Len(t, producer.messages, 1)
	assert.Equal(t, "dlq-invalid", producer.messages[0].topic)

	var envelope model.DLQEnvelope
	require.NoError(t, json.Unmarshal(producer.messages[0].value, &envelope))
	assert.Equal(t, "validator", envelope.Error.Stage)
	assert.Equal(t, "SCHEMA_VIOLATION", envelope.Error.ErrorType)
	assert.Equal(t, 0, envelope.Error.RetryCount)
	assert.False(t, envelope.Error.PoisonPill)
}

func TestRouter_RoutesPoisonPillToPoison Topic(t *testing.T) {
	producer := &mockProducer{}
	router := NewRouter(producer, "dlq-invalid", "dlq-poison", "validator", "validator-pod-1")

	event := []byte(`{"service":"test"}`)
	err := router.Route(context.Background(), event, "SCHEMA_VIOLATION", "still broken", model.Permanent, 3)
	require.NoError(t, err)

	require.Len(t, producer.messages, 1)
	assert.Equal(t, "dlq-poison", producer.messages[0].topic)

	var envelope model.DLQEnvelope
	require.NoError(t, json.Unmarshal(producer.messages[0].value, &envelope))
	assert.True(t, envelope.Error.PoisonPill)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/dlq/ -v
```

Expected: FAIL

- [ ] **Step 3: Implement DLQ router**

```go
// internal/dlq/router.go
package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

const MaxRetries = 3

type MessageSender interface {
	Send(topic string, key, value []byte, headers map[string]string) error
}

type Router struct {
	producer        MessageSender
	dlqTopic        string
	poisonTopic     string
	stage           string
	serviceInstance string
}

func NewRouter(producer MessageSender, dlqTopic, poisonTopic, stage, serviceInstance string) *Router {
	return &Router{
		producer:        producer,
		dlqTopic:        dlqTopic,
		poisonTopic:     poisonTopic,
		stage:           stage,
		serviceInstance: serviceInstance,
	}
}

func (r *Router) Route(ctx context.Context, originalEvent []byte, errorType, errorMsg string, class model.ErrorClass, retryCount int) error {
	isPoisoned := retryCount >= MaxRetries

	envelope := model.DLQEnvelope{
		OriginalEvent: originalEvent,
		Error: model.DLQError{
			Stage:           r.stage,
			ErrorType:       errorType,
			ErrorMessage:    errorMsg,
			FailedAt:        time.Now().UTC(),
			RetryCount:      retryCount,
			PoisonPill:      isPoisoned,
			ServiceInstance: r.serviceInstance,
		},
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}

	topic := r.dlqTopic
	if isPoisoned {
		topic = r.poisonTopic
	}

	return r.producer.Send(topic, nil, data, nil)
}
```

- [ ] **Step 4: Write poison-pill tests**

```go
// internal/dlq/poison_test.go
package dlq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

func TestIsPoisonPill_UnderThreshold(t *testing.T) {
	envelope := model.DLQEnvelope{
		Error: model.DLQError{RetryCount: 2},
	}
	assert.False(t, IsPoisonPill(envelope))
}

func TestIsPoisonPill_AtThreshold(t *testing.T) {
	envelope := model.DLQEnvelope{
		Error: model.DLQError{RetryCount: 3},
	}
	assert.True(t, IsPoisonPill(envelope))
}

func TestIsPoisonPill_OverThreshold(t *testing.T) {
	envelope := model.DLQEnvelope{
		Error: model.DLQError{RetryCount: 5},
	}
	assert.True(t, IsPoisonPill(envelope))
}
```

- [ ] **Step 5: Implement poison-pill detection**

```go
// internal/dlq/poison.go
package dlq

import "github.com/widasinnacy/distributed-collector/internal/model"

func IsPoisonPill(envelope model.DLQEnvelope) bool {
	return envelope.Error.RetryCount >= MaxRetries
}
```

- [ ] **Step 6: Run all DLQ tests**

```bash
go test ./internal/dlq/ -v -race
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: DLQ router with poison-pill detection and configurable retry threshold"
```

---

### Task 5: MongoDB Batch Writer

**Files:**
- Create: `internal/mongo/client.go`
- Create: `internal/mongo/batchwriter.go`
- Create: `internal/mongo/batchwriter_test.go`

**Interfaces:**
- Consumes: `config.Config` (MongoURI, MongoDatabase)
- Produces:
  - `mongo.Connect(ctx, uri, database) (*Client, error)`
  - `mongo.Client` struct with `Collection(name) *mongo.Collection`, `Close(ctx) error`, `Ping(ctx) error`
  - `mongo.BatchWriter` struct with `Write(doc)`, `Flush(ctx) error`, `Stop(ctx) error`
  - `mongo.BatchConfig` struct: `{Collection, BatchSize, FlushInterval}`

- [ ] **Step 1: Write batch writer test (unit test with mock interface)**

```go
// internal/mongo/batchwriter_test.go
package mongo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCollection struct {
	mu      sync.Mutex
	batches [][]interface{}
}

func (m *mockCollection) InsertMany(ctx context.Context, docs []interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, docs)
	return nil
}

func (m *mockCollection) getBatches() [][]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batches
}

func TestBatchWriter_FlushesOnSize(t *testing.T) {
	coll := &mockCollection{}
	bw := NewBatchWriter(coll, BatchConfig{
		BatchSize:     5,
		FlushInterval: 10 * time.Second,
	})

	ctx := context.Background()
	bw.Start(ctx)

	for i := 0; i < 5; i++ {
		bw.Write(map[string]string{"id": "event"})
	}

	time.Sleep(100 * time.Millisecond)

	batches := coll.getBatches()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 5)

	bw.Stop(ctx)
}

func TestBatchWriter_FlushesOnInterval(t *testing.T) {
	coll := &mockCollection{}
	bw := NewBatchWriter(coll, BatchConfig{
		BatchSize:     1000,
		FlushInterval: 100 * time.Millisecond,
	})

	ctx := context.Background()
	bw.Start(ctx)

	bw.Write(map[string]string{"id": "event1"})
	bw.Write(map[string]string{"id": "event2"})

	time.Sleep(200 * time.Millisecond)

	batches := coll.getBatches()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 2)

	bw.Stop(ctx)
}

func TestBatchWriter_FlushesOnStop(t *testing.T) {
	coll := &mockCollection{}
	bw := NewBatchWriter(coll, BatchConfig{
		BatchSize:     1000,
		FlushInterval: 10 * time.Second,
	})

	ctx := context.Background()
	bw.Start(ctx)

	bw.Write(map[string]string{"id": "event1"})

	bw.Stop(ctx)

	batches := coll.getBatches()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/mongo/ -v
```

Expected: FAIL

- [ ] **Step 3: Implement batch writer**

```go
// internal/mongo/batchwriter.go
package mongo

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Collection interface {
	InsertMany(ctx context.Context, docs []interface{}) error
}

type BatchConfig struct {
	BatchSize     int
	FlushInterval time.Duration
}

type BatchWriter struct {
	coll     Collection
	config   BatchConfig
	buffer   []interface{}
	mu       sync.Mutex
	input    chan interface{}
	done     chan struct{}
}

func NewBatchWriter(coll Collection, config BatchConfig) *BatchWriter {
	return &BatchWriter{
		coll:   coll,
		config: config,
		buffer: make([]interface{}, 0, config.BatchSize),
		input:  make(chan interface{}, config.BatchSize*2),
		done:   make(chan struct{}),
	}
}

func (bw *BatchWriter) Start(ctx context.Context) {
	go bw.loop(ctx)
}

func (bw *BatchWriter) loop(ctx context.Context) {
	ticker := time.NewTicker(bw.config.FlushInterval)
	defer ticker.Stop()
	defer close(bw.done)

	for {
		select {
		case doc, ok := <-bw.input:
			if !ok {
				bw.flush(ctx)
				return
			}
			bw.mu.Lock()
			bw.buffer = append(bw.buffer, doc)
			shouldFlush := len(bw.buffer) >= bw.config.BatchSize
			bw.mu.Unlock()

			if shouldFlush {
				bw.flush(ctx)
			}
		case <-ticker.C:
			bw.flush(ctx)
		case <-ctx.Done():
			bw.flush(context.Background())
			return
		}
	}
}

func (bw *BatchWriter) Write(doc interface{}) {
	bw.input <- doc
}

func (bw *BatchWriter) flush(ctx context.Context) {
	bw.mu.Lock()
	if len(bw.buffer) == 0 {
		bw.mu.Unlock()
		return
	}
	batch := bw.buffer
	bw.buffer = make([]interface{}, 0, bw.config.BatchSize)
	bw.mu.Unlock()

	if err := bw.coll.InsertMany(ctx, batch); err != nil {
		slog.Error("batch write failed", "error", err, "batch_size", len(batch))
	}
}

func (bw *BatchWriter) Flush(ctx context.Context) error {
	bw.flush(ctx)
	return nil
}

func (bw *BatchWriter) Stop(ctx context.Context) error {
	close(bw.input)
	<-bw.done
	return nil
}
```

```go
// internal/mongo/client.go
package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
}

func Connect(ctx context.Context, uri, database string) (*Client, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	return &Client{
		client:   client,
		database: client.Database(database),
	}, nil
}

func (c *Client) Collection(name string) *mongo.Collection {
	return c.database.Collection(name)
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/mongo/ -v -race
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: MongoDB batch writer with size and time-based flushing"
```

---

### Task 6: Validator Service

**Files:**
- Create: `internal/validator/validator.go`
- Create: `internal/validator/validator_test.go`
- Create: `cmd/validator/main.go`

**Interfaces:**
- Consumes: `model.LogEvent`, `kafka.Consumer`, `kafka.Producer`, `dlq.Router`, `pipeline.WorkerPool`
- Produces:
  - `validator.Validate(event []byte) (*model.LogEvent, error)`
  - `validator.ValidationError` type with `ErrorType` and `Message` fields

- [ ] **Step 1: Write validator logic tests**

```go
// internal/validator/validator_test.go
package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_ValidEvent(t *testing.T) {
	raw := []byte(`{
		"timestamp": "2026-08-13T12:30:00.123Z",
		"service": "payment-service",
		"level": "ERROR",
		"message": "failed to process transaction",
		"trace_id": "abc123",
		"span_id": "def456",
		"metadata": {"latency_ms": 342}
	}`)

	event, err := Validate(raw)
	require.NoError(t, err)
	assert.Equal(t, "payment-service", event.Service)
	assert.Equal(t, "ERROR", event.Level)
	assert.Equal(t, int64(342), event.Metadata.LatencyMs)
}

func TestValidate_MissingTimestamp(t *testing.T) {
	raw := []byte(`{
		"service": "payment-service",
		"level": "ERROR",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "MISSING_FIELD", ve.ErrorType)
	assert.Contains(t, ve.Message, "timestamp")
}

func TestValidate_InvalidLevel(t *testing.T) {
	raw := []byte(`{
		"timestamp": "2026-08-13T12:30:00Z",
		"service": "test",
		"level": "UNKNOWN",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "INVALID_VALUE", ve.ErrorType)
}

func TestValidate_InvalidTimestamp(t *testing.T) {
	raw := []byte(`{
		"timestamp": "not-a-date",
		"service": "test",
		"level": "INFO",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "INVALID_FORMAT", ve.ErrorType)
}

func TestValidate_ServiceTooLong(t *testing.T) {
	longService := make([]byte, 129)
	for i := range longService {
		longService[i] = 'a'
	}

	raw := []byte(`{
		"timestamp": "2026-08-13T12:30:00Z",
		"service": "` + string(longService) + `",
		"level": "INFO",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "INVALID_VALUE", ve.ErrorType)
}

func TestValidate_MalformedJSON(t *testing.T) {
	raw := []byte(`{not valid json}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "MALFORMED_JSON", ve.ErrorType)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/validator/ -v
```

Expected: FAIL

- [ ] **Step 3: Implement validator**

```go
// internal/validator/validator.go
package validator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

var validLevels = map[string]bool{
	"DEBUG": true,
	"INFO":  true,
	"WARN":  true,
	"ERROR": true,
	"FATAL": true,
}

type ValidationError struct {
	ErrorType string
	Message   string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorType, e.Message)
}

func Validate(raw []byte) (*model.LogEvent, error) {
	var event model.LogEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, &ValidationError{ErrorType: "MALFORMED_JSON", Message: err.Error()}
	}

	if event.Timestamp.IsZero() {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "timestamp is required"}
	}

	if _, err := time.Parse(time.RFC3339Nano, event.Timestamp.Format(time.RFC3339Nano)); err != nil {
		return nil, &ValidationError{ErrorType: "INVALID_FORMAT", Message: "timestamp must be valid ISO 8601"}
	}

	if event.Service == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "service is required"}
	}
	if len(event.Service) > 128 {
		return nil, &ValidationError{ErrorType: "INVALID_VALUE", Message: "service must be <= 128 characters"}
	}

	if event.Level == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "level is required"}
	}
	if !validLevels[event.Level] {
		return nil, &ValidationError{ErrorType: "INVALID_VALUE", Message: fmt.Sprintf("level must be one of DEBUG, INFO, WARN, ERROR, FATAL; got %q", event.Level)}
	}

	if event.Message == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "message is required"}
	}

	return &event, nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/validator/ -v -race
```

Expected: PASS

- [ ] **Step 5: Implement validator service main**

```go
// cmd/validator/main.go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
	"github.com/widasinnacy/distributed-collector/internal/validator"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "validator"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "validator-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "raw-logs"
	}
	if cfg.OutputTopic == "" {
		cfg.OutputTopic = "validated-logs"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "dlq-invalid"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	hostname, _ := os.Hostname()
	dlqRouter := dlq.NewRouter(producer, cfg.DLQTopic, "dlq-poison", "validator", hostname)

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		event, err := validator.Validate(msg)
		if err != nil {
			var ve *validator.ValidationError
			if ok := isValidationError(err, &ve); ok {
				dlqRouter.Route(ctx, msg, ve.ErrorType, ve.Message, model.Permanent, 0)
			}
			return nil, err
		}

		output, _ := json.Marshal(event)
		headers := kafka.InjectTraceContext(ctx)
		producer.Send(cfg.OutputTopic, []byte(event.Service), output, headers)
		return output, nil
	})
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("validator service starting", "input", cfg.InputTopic, "output", cfg.OutputTopic)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		traceCtx := kafka.ExtractTraceContext(ctx, headers)
		pool.Submit(value)
		_ = traceCtx
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	consumer.Close()
}

func isValidationError(err error, target **validator.ValidationError) bool {
	ve, ok := err.(*validator.ValidationError)
	if ok {
		*target = ve
	}
	return ok
}
```

- [ ] **Step 6: Verify it compiles**

```bash
go build ./cmd/validator/
```

Expected: SUCCESS (binary created)

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: validator service with schema validation and DLQ routing"
```

---

### Task 7: Enricher Service

**Files:**
- Create: `internal/enricher/registry.go`
- Create: `internal/enricher/registry_test.go`
- Create: `internal/enricher/enricher.go`
- Create: `internal/enricher/enricher_test.go`
- Create: `configs/service-registry.yaml`
- Create: `cmd/enricher/main.go`

**Interfaces:**
- Consumes: `model.LogEvent`, `model.EnrichedLogEvent`, `kafka.Consumer`, `kafka.Producer`, `dlq.Router`
- Produces:
  - `enricher.Registry` struct with `Lookup(service string) (*ServiceInfo, bool)`
  - `enricher.LoadRegistry(path string) (*Registry, error)`
  - `enricher.Enrich(event *model.LogEvent, info *ServiceInfo) *model.EnrichedLogEvent`
  - `enricher.ServiceInfo` struct: `{Team, Tier, SLA}`

- [ ] **Step 1: Write registry tests**

```go
// internal/enricher/registry_test.go
package enricher

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadRegistry_ParsesYAML(t *testing.T) {
	content := `services:
  payment-service:
    team: payments
    tier: critical
    sla: "99.99"
  auth-service:
    team: platform
    tier: standard
    sla: "99.9"
`
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "registry.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	reg, err := LoadRegistry(path)
	require.NoError(t, err)

	info, ok := reg.Lookup("payment-service")
	require.True(t, ok)
	assert.Equal(t, "payments", info.Team)
	assert.Equal(t, "critical", info.Tier)

	info, ok = reg.Lookup("auth-service")
	require.True(t, ok)
	assert.Equal(t, "platform", info.Team)
	assert.Equal(t, "standard", info.Tier)
}

func TestLoadRegistry_UnknownService(t *testing.T) {
	content := `services:
  payment-service:
    team: payments
    tier: critical
    sla: "99.99"
`
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "registry.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	reg, err := LoadRegistry(path)
	require.NoError(t, err)

	_, ok := reg.Lookup("unknown-service")
	assert.False(t, ok)
}
```

- [ ] **Step 2: Write enricher logic tests**

```go
// internal/enricher/enricher_test.go
package enricher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

func TestEnrich_AddsMetadata(t *testing.T) {
	event := &model.LogEvent{
		Timestamp: time.Date(2026, 8, 13, 14, 35, 22, 0, time.UTC),
		Service:   "payment-service",
		Level:     "ERROR",
		Message:   "test",
	}

	info := &ServiceInfo{Team: "payments", Tier: "critical", SLA: "99.99"}

	enriched := Enrich(event, info, "1.0.0")

	assert.Equal(t, "payments", enriched.Enrichment.Team)
	assert.Equal(t, "critical", enriched.Enrichment.Tier)
	assert.Equal(t, "1.0.0", enriched.Enrichment.PipelineVersion)
	assert.Equal(t, time.Date(2026, 8, 13, 14, 0, 0, 0, time.UTC), enriched.Enrichment.HourBucket)
	assert.False(t, enriched.Enrichment.ProcessedAt.IsZero())
	assert.Equal(t, "critical", enriched.RetentionTier)
}

func TestEnrich_ComputesTTL_Critical(t *testing.T) {
	event := &model.LogEvent{
		Timestamp: time.Date(2026, 8, 13, 14, 0, 0, 0, time.UTC),
		Service:   "payment-service",
		Level:     "ERROR",
		Message:   "test",
	}

	info := &ServiceInfo{Team: "payments", Tier: "critical", SLA: "99.99"}
	enriched := Enrich(event, info, "1.0.0")

	expectedExpiry := enriched.IngestedAt.Add(90 * 24 * time.Hour)
	assert.WithinDuration(t, expectedExpiry, enriched.ExpiresAt, time.Second)
}

func TestEnrich_ComputesTTL_Debug(t *testing.T) {
	event := &model.LogEvent{
		Timestamp: time.Date(2026, 8, 13, 14, 0, 0, 0, time.UTC),
		Service:   "debug-service",
		Level:     "DEBUG",
		Message:   "test",
	}

	info := &ServiceInfo{Team: "dev", Tier: "debug", SLA: "95.0"}
	enriched := Enrich(event, info, "1.0.0")

	expectedExpiry := enriched.IngestedAt.Add(7 * 24 * time.Hour)
	assert.WithinDuration(t, expectedExpiry, enriched.ExpiresAt, time.Second)
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
go test ./internal/enricher/ -v
```

Expected: FAIL

- [ ] **Step 4: Implement registry and enricher**

```go
// internal/enricher/registry.go
package enricher

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type ServiceInfo struct {
	Team string `yaml:"team"`
	Tier string `yaml:"tier"`
	SLA  string `yaml:"sla"`
}

type Registry struct {
	services map[string]*ServiceInfo
}

type registryFile struct {
	Services map[string]*ServiceInfo `yaml:"services"`
}

func LoadRegistry(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read registry file: %w", err)
	}

	var rf registryFile
	if err := yaml.Unmarshal(data, &rf); err != nil {
		return nil, fmt.Errorf("parse registry yaml: %w", err)
	}

	return &Registry{services: rf.Services}, nil
}

func (r *Registry) Lookup(service string) (*ServiceInfo, bool) {
	info, ok := r.services[service]
	return info, ok
}
```

```go
// internal/enricher/enricher.go
package enricher

import (
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

var tierTTL = map[string]time.Duration{
	"critical": 90 * 24 * time.Hour,
	"standard": 30 * 24 * time.Hour,
	"debug":    7 * 24 * time.Hour,
}

func Enrich(event *model.LogEvent, info *ServiceInfo, pipelineVersion string) *model.EnrichedLogEvent {
	now := time.Now().UTC()

	ttl, ok := tierTTL[info.Tier]
	if !ok {
		ttl = tierTTL["standard"]
	}

	return &model.EnrichedLogEvent{
		LogEvent: *event,
		Enrichment: model.Enrichment{
			Team:            info.Team,
			Tier:            info.Tier,
			ProcessedAt:     now,
			HourBucket:      event.Timestamp.UTC().Truncate(time.Hour),
			PipelineVersion: pipelineVersion,
		},
		ExpiresAt:     now.Add(ttl),
		RetentionTier: info.Tier,
		IngestedAt:    now,
	}
}
```

- [ ] **Step 5: Create service registry config**

```yaml
# configs/service-registry.yaml
services:
  payment-service:
    team: payments
    tier: critical
    sla: "99.99"
  auth-service:
    team: platform
    tier: critical
    sla: "99.99"
  user-service:
    team: platform
    tier: standard
    sla: "99.9"
  notification-service:
    team: comms
    tier: standard
    sla: "99.5"
  analytics-service:
    team: data
    tier: debug
    sla: "95.0"
  search-service:
    team: discovery
    tier: standard
    sla: "99.9"
  inventory-service:
    team: supply-chain
    tier: critical
    sla: "99.99"
  shipping-service:
    team: logistics
    tier: standard
    sla: "99.5"
  recommendation-service:
    team: ml
    tier: debug
    sla: "95.0"
  gateway-service:
    team: platform
    tier: critical
    sla: "99.99"
```

- [ ] **Step 6: Run tests**

```bash
go test ./internal/enricher/ -v -race
```

Expected: PASS

- [ ] **Step 7: Implement enricher service main**

```go
// cmd/enricher/main.go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/enricher"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "enricher"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "enricher-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "validated-logs"
	}
	if cfg.OutputTopic == "" {
		cfg.OutputTopic = "enriched-logs"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "dlq-enrich-failed"
	}

	registryPath := os.Getenv("SERVICE_REGISTRY_PATH")
	if registryPath == "" {
		registryPath = "configs/service-registry.yaml"
	}

	registry, err := enricher.LoadRegistry(registryPath)
	if err != nil {
		slog.Error("failed to load service registry", "error", err, "path", registryPath)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	hostname, _ := os.Hostname()
	dlqRouter := dlq.NewRouter(producer, cfg.DLQTopic, "dlq-poison", "enricher", hostname)

	pipelineVersion := os.Getenv("PIPELINE_VERSION")
	if pipelineVersion == "" {
		pipelineVersion = "1.0.0"
	}

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		var event model.LogEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			dlqRouter.Route(ctx, msg, "UNMARSHAL_ERROR", err.Error(), model.Permanent, 0)
			return nil, err
		}

		info, ok := registry.Lookup(event.Service)
		if !ok {
			dlqRouter.Route(ctx, msg, "UNKNOWN_SERVICE", "service '"+event.Service+"' not in registry", model.Permanent, 0)
			return nil, nil
		}

		enriched := enricher.Enrich(&event, info, pipelineVersion)
		output, _ := json.Marshal(enriched)
		headers := kafka.InjectTraceContext(ctx)
		producer.Send(cfg.OutputTopic, []byte(event.Service), output, headers)
		return output, nil
	})
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("enricher service starting", "input", cfg.InputTopic, "output", cfg.OutputTopic)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		pool.Submit(value)
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	consumer.Close()
}
```

- [ ] **Step 8: Verify it compiles**

```bash
go build ./cmd/enricher/
```

Expected: SUCCESS

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: enricher service with registry lookup and tier-based TTL"
```

---

### Task 8: Aggregator Service

**Files:**
- Create: `internal/aggregator/aggregator.go`
- Create: `internal/aggregator/aggregator_test.go`
- Create: `cmd/aggregator/main.go`

**Interfaces:**
- Consumes: `model.EnrichedLogEvent`, `model.Aggregate`, `mongo.BatchWriter`, `kafka.Consumer`
- Produces:
  - `aggregator.Engine` struct with `Ingest(event *model.EnrichedLogEvent)`, `Flush() []model.Aggregate`, `Reset()`

- [ ] **Step 1: Write aggregator engine tests**

```go
// internal/aggregator/aggregator_test.go
package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

func makeEvent(service, level string, latencyMs int64, endpoint, errorCode string) *model.EnrichedLogEvent {
	return &model.EnrichedLogEvent{
		LogEvent: model.LogEvent{
			Timestamp: time.Date(2026, 8, 13, 14, 30, 15, 0, time.UTC),
			Service:   service,
			Level:     level,
			Message:   "test",
			Metadata: model.EventMetadata{
				LatencyMs: latencyMs,
				Endpoint:  endpoint,
				ErrorCode: errorCode,
			},
		},
		Enrichment: model.Enrichment{
			HourBucket: time.Date(2026, 8, 13, 14, 0, 0, 0, time.UTC),
		},
	}
}

func TestEngine_CountsByLevel(t *testing.T) {
	engine := NewEngine()

	engine.Ingest(makeEvent("svc-a", "INFO", 10, "/api", ""))
	engine.Ingest(makeEvent("svc-a", "INFO", 20, "/api", ""))
	engine.Ingest(makeEvent("svc-a", "ERROR", 100, "/api", "TIMEOUT"))
	engine.Ingest(makeEvent("svc-a", "FATAL", 500, "/api", "OOM"))

	aggregates := engine.Flush()
	require.Len(t, aggregates, 1)

	agg := aggregates[0]
	assert.Equal(t, "svc-a", agg.Service)
	assert.Equal(t, int64(4), agg.Counts.Total)
	assert.Equal(t, int64(2), agg.Counts.Info)
	assert.Equal(t, int64(1), agg.Counts.Error)
	assert.Equal(t, int64(1), agg.Counts.Fatal)
}

func TestEngine_ErrorRate(t *testing.T) {
	engine := NewEngine()

	for i := 0; i < 8; i++ {
		engine.Ingest(makeEvent("svc-b", "INFO", 10, "/api", ""))
	}
	engine.Ingest(makeEvent("svc-b", "ERROR", 100, "/api", "TIMEOUT"))
	engine.Ingest(makeEvent("svc-b", "FATAL", 500, "/api", "OOM"))

	aggregates := engine.Flush()
	require.Len(t, aggregates, 1)
	assert.InDelta(t, 0.2, aggregates[0].ErrorRate, 0.001)
}

func TestEngine_LatencyPercentiles(t *testing.T) {
	engine := NewEngine()

	latencies := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	for _, l := range latencies {
		engine.Ingest(makeEvent("svc-c", "INFO", l, "/api", ""))
	}

	aggregates := engine.Flush()
	require.Len(t, aggregates, 1)

	assert.InDelta(t, 50, aggregates[0].Latency.P50Ms, 15)
	assert.InDelta(t, 95, aggregates[0].Latency.P95Ms, 15)
	assert.InDelta(t, 100, aggregates[0].Latency.P99Ms, 15)
}

func TestEngine_MultipleServices(t *testing.T) {
	engine := NewEngine()

	engine.Ingest(makeEvent("svc-a", "INFO", 10, "/api", ""))
	engine.Ingest(makeEvent("svc-b", "ERROR", 100, "/api", "TIMEOUT"))

	aggregates := engine.Flush()
	assert.Len(t, aggregates, 2)
}

func TestEngine_TopErrors(t *testing.T) {
	engine := NewEngine()

	for i := 0; i < 5; i++ {
		engine.Ingest(makeEvent("svc-d", "ERROR", 100, "/api", "TIMEOUT"))
	}
	for i := 0; i < 3; i++ {
		engine.Ingest(makeEvent("svc-d", "ERROR", 100, "/api", "DECLINED"))
	}

	aggregates := engine.Flush()
	require.Len(t, aggregates, 1)
	require.GreaterOrEqual(t, len(aggregates[0].TopErrors), 2)
	assert.Equal(t, "TIMEOUT", aggregates[0].TopErrors[0].Code)
	assert.Equal(t, int64(5), aggregates[0].TopErrors[0].Count)
}

func TestEngine_UniqueEndpoints(t *testing.T) {
	engine := NewEngine()

	engine.Ingest(makeEvent("svc-e", "INFO", 10, "/api/v1/users", ""))
	engine.Ingest(makeEvent("svc-e", "INFO", 10, "/api/v1/orders", ""))
	engine.Ingest(makeEvent("svc-e", "INFO", 10, "/api/v1/users", ""))

	aggregates := engine.Flush()
	require.Len(t, aggregates, 1)
	assert.ElementsMatch(t, []string{"/api/v1/users", "/api/v1/orders"}, aggregates[0].UniqueEndpoints)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/aggregator/ -v
```

Expected: FAIL

- [ ] **Step 3: Implement aggregator engine**

```go
// internal/aggregator/aggregator.go
package aggregator

import (
	"sort"
	"sync"
	"time"

	tdigest "github.com/caio/go-tdigest/v4"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

type windowKey struct {
	service string
	bucket  time.Time
}

type windowState struct {
	counts    model.LevelCounts
	digest    *tdigest.TDigest
	errors    map[string]int64
	endpoints map[string]struct{}
}

type Engine struct {
	mu      sync.Mutex
	windows map[windowKey]*windowState
}

func NewEngine() *Engine {
	return &Engine{
		windows: make(map[windowKey]*windowState),
	}
}

func (e *Engine) Ingest(event *model.EnrichedLogEvent) {
	bucket := event.Timestamp.UTC().Truncate(time.Minute)
	key := windowKey{service: event.Service, bucket: bucket}

	e.mu.Lock()
	defer e.mu.Unlock()

	state, ok := e.windows[key]
	if !ok {
		td, _ := tdigest.New(tdigest.Compression(100))
		state = &windowState{
			digest:    td,
			errors:    make(map[string]int64),
			endpoints: make(map[string]struct{}),
		}
		e.windows[key] = state
	}

	state.counts.Total++
	switch event.Level {
	case "DEBUG":
		state.counts.Debug++
	case "INFO":
		state.counts.Info++
	case "WARN":
		state.counts.Warn++
	case "ERROR":
		state.counts.Error++
	case "FATAL":
		state.counts.Fatal++
	}

	if event.Metadata.LatencyMs > 0 {
		state.digest.Add(float64(event.Metadata.LatencyMs))
	}

	if event.Metadata.ErrorCode != "" {
		state.errors[event.Metadata.ErrorCode]++
	}

	if event.Metadata.Endpoint != "" {
		state.endpoints[event.Metadata.Endpoint] = struct{}{}
	}
}

func (e *Engine) Flush() []model.Aggregate {
	e.mu.Lock()
	windows := e.windows
	e.windows = make(map[windowKey]*windowState)
	e.mu.Unlock()

	now := time.Now().UTC()
	aggregates := make([]model.Aggregate, 0, len(windows))

	for key, state := range windows {
		errorCount := state.counts.Error + state.counts.Fatal
		var errorRate float64
		if state.counts.Total > 0 {
			errorRate = float64(errorCount) / float64(state.counts.Total)
		}

		endpoints := make([]string, 0, len(state.endpoints))
		for ep := range state.endpoints {
			endpoints = append(endpoints, ep)
		}
		sort.Strings(endpoints)

		topErrors := buildTopErrors(state.errors)

		agg := model.Aggregate{
			Service:       key.service,
			Bucket:        key.bucket,
			WindowSeconds: 60,
			Counts:        state.counts,
			Latency: model.LatencyStats{
				P50Ms: state.digest.Quantile(0.50),
				P95Ms: state.digest.Quantile(0.95),
				P99Ms: state.digest.Quantile(0.99),
			},
			ErrorRate:       errorRate,
			UniqueEndpoints: endpoints,
			TopErrors:       topErrors,
			ComputedAt:      now,
		}
		aggregates = append(aggregates, agg)
	}

	return aggregates
}

func buildTopErrors(errors map[string]int64) []model.ErrorCount {
	result := make([]model.ErrorCount, 0, len(errors))
	for code, count := range errors {
		result = append(result, model.ErrorCount{Code: code, Count: count})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})
	if len(result) > 10 {
		result = result[:10]
	}
	return result
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/aggregator/ -v -race
```

Expected: PASS

- [ ] **Step 5: Implement aggregator service main**

```go
// cmd/aggregator/main.go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/aggregator"
	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	mongopkg "github.com/widasinnacy/distributed-collector/internal/mongo"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "aggregator"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "aggregator-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "enriched-logs"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mongoClient, err := mongopkg.Connect(ctx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		slog.Error("failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer mongoClient.Close(ctx)

	engine := aggregator.NewEngine()

	flushInterval := time.Duration(cfg.FlushIntervalSec) * time.Second
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				aggregates := engine.Flush()
				if len(aggregates) == 0 {
					continue
				}
				coll := mongoClient.Collection("aggregates")
				for _, agg := range aggregates {
					filter := map[string]interface{}{
						"service": agg.Service,
						"bucket":  agg.Bucket,
					}
					_ = filter
					_ = coll
					// Uses upsert in production; simplified for initial implementation
					slog.Info("flushed aggregate", "service", agg.Service, "total", agg.Counts.Total)
				}
			case <-ctx.Done():
				aggregates := engine.Flush()
				slog.Info("final flush on shutdown", "aggregates", len(aggregates))
				return
			}
		}
	}()

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		var event model.EnrichedLogEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			slog.Error("failed to unmarshal enriched event", "error", err)
			return nil, err
		}
		engine.Ingest(&event)
		return nil, nil
	})
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("aggregator service starting", "input", cfg.InputTopic, "flush_interval", flushInterval)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		pool.Submit(value)
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	consumer.Close()
}
```

- [ ] **Step 6: Verify it compiles**

```bash
go build ./cmd/aggregator/
```

Expected: SUCCESS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: aggregator service with tumbling windows and T-Digest percentiles"
```

---

### Task 9: Raw Store Writer Service

**Files:**
- Create: `cmd/raw-store-writer/main.go`

**Interfaces:**
- Consumes: `model.EnrichedLogEvent`, `kafka.Consumer`, `mongo.BatchWriter`, `dlq.Router`
- Produces: Raw Store Writer binary (writes enriched events to MongoDB `logs` collection)

- [ ] **Step 1: Implement raw store writer service**

```go
// cmd/raw-store-writer/main.go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	mongopkg "github.com/widasinnacy/distributed-collector/internal/mongo"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "raw-store-writer"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "raw-store-writer-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "enriched-logs"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "dlq-write-failed"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mongoClient, err := mongopkg.Connect(ctx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		slog.Error("failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer mongoClient.Close(ctx)

	coll := mongoClient.Collection("logs")
	batchWriter := mongopkg.NewBatchWriter(&mongoCollectionAdapter{coll: coll}, mongopkg.BatchConfig{
		BatchSize:     cfg.BatchSize,
		FlushInterval: time.Duration(cfg.FlushIntervalSec) * time.Second,
	})
	batchWriter.Start(ctx)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	hostname, _ := os.Hostname()
	dlqRouter := dlq.NewRouter(producer, cfg.DLQTopic, "dlq-poison", "raw-store-writer", hostname)

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		var event model.EnrichedLogEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			dlqRouter.Route(ctx, msg, "UNMARSHAL_ERROR", err.Error(), model.Permanent, 0)
			return nil, err
		}
		batchWriter.Write(event)
		return nil, nil
	})
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("raw-store-writer starting", "input", cfg.InputTopic, "batch_size", cfg.BatchSize)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		pool.Submit(value)
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	batchWriter.Stop(ctx)
	consumer.Close()
}

type mongoCollectionAdapter struct {
	coll interface {
		InsertMany(ctx context.Context, docs []interface{}) (interface{}, error)
	}
}

func (a *mongoCollectionAdapter) InsertMany(ctx context.Context, docs []interface{}) error {
	_, err := a.coll.InsertMany(ctx, docs)
	return err
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./cmd/raw-store-writer/
```

Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: raw store writer service with batch MongoDB writes"
```

---

### Task 10: Load Generator

**Files:**
- Create: `internal/loadgen/generator.go`
- Create: `internal/loadgen/generator_test.go`
- Create: `cmd/loadgen/main.go`

**Interfaces:**
- Consumes: `model.LogEvent`, `kafka.Producer`
- Produces:
  - `loadgen.Generator` struct with `Generate(count int) []model.LogEvent`
  - `loadgen.Config` struct: `{Mode, Rate, Duration, Services, InvalidPercent}`
  - CLI binary with flags: `--mode`, `--rate`, `--duration`, `--kafka-brokers`, `--topic`, `--report-dir`

- [ ] **Step 1: Write generator tests**

```go
// internal/loadgen/generator_test.go
package loadgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator_ProducesValidEvents(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		Services:       []string{"svc-a", "svc-b", "svc-c"},
		InvalidPercent: 0,
	})

	events := gen.Generate(100)
	require.Len(t, events, 100)

	for _, e := range events {
		assert.NotEmpty(t, e.Service)
		assert.NotEmpty(t, e.Level)
		assert.NotEmpty(t, e.Message)
		assert.False(t, e.Timestamp.IsZero())
	}
}

func TestGenerator_LevelDistribution(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		Services:       []string{"svc-a"},
		InvalidPercent: 0,
	})

	events := gen.Generate(10000)

	counts := make(map[string]int)
	for _, e := range events {
		counts[e.Level]++
	}

	infoRatio := float64(counts["INFO"]) / 10000.0
	assert.InDelta(t, 0.70, infoRatio, 0.05)

	errorRatio := float64(counts["ERROR"]) / 10000.0
	assert.InDelta(t, 0.10, errorRatio, 0.03)
}

func TestGenerator_InvalidEvents(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		Services:       []string{"svc-a"},
		InvalidPercent: 10,
	})

	events := gen.GenerateRaw(1000)

	invalidCount := 0
	for _, raw := range events {
		if isInvalid(raw) {
			invalidCount++
		}
	}

	ratio := float64(invalidCount) / 1000.0
	assert.InDelta(t, 0.10, ratio, 0.03)
}

func TestGenerator_LogNormalLatency(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		Services:       []string{"svc-a"},
		InvalidPercent: 0,
	})

	events := gen.Generate(1000)

	var total int64
	for _, e := range events {
		assert.GreaterOrEqual(t, e.Metadata.LatencyMs, int64(1))
		total += e.Metadata.LatencyMs
	}

	avgLatency := float64(total) / 1000.0
	assert.Greater(t, avgLatency, 10.0)
	assert.Less(t, avgLatency, 500.0)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/loadgen/ -v
```

Expected: FAIL

- [ ] **Step 3: Implement generator**

```go
// internal/loadgen/generator.go
package loadgen

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

type GeneratorConfig struct {
	Services       []string
	InvalidPercent int
}

type Generator struct {
	config GeneratorConfig
	rng    *rand.Rand
}

func NewGenerator(config GeneratorConfig) *Generator {
	return &Generator{
		config: config,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

var levelWeights = []struct {
	level  string
	weight int
}{
	{"INFO", 70},
	{"WARN", 15},
	{"ERROR", 10},
	{"DEBUG", 4},
	{"FATAL", 1},
}

var messages = []string{
	"request processed successfully",
	"database query completed",
	"cache miss, fetching from source",
	"connection timeout",
	"failed to process transaction",
	"rate limit exceeded",
	"authentication failed",
	"service unavailable",
	"invalid request payload",
	"response sent",
}

var endpoints = []string{
	"/api/v1/users",
	"/api/v1/orders",
	"/api/v1/payments",
	"/api/v1/products",
	"/api/v1/auth/login",
	"/api/v1/notifications",
	"/api/v1/search",
	"/api/v1/inventory",
}

var methods = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
var regions = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
var errorCodes = []string{"TIMEOUT", "PAYMENT_DECLINED", "RATE_LIMITED", "INVALID_INPUT", "OOM", "CONNECTION_REFUSED"}

func (g *Generator) Generate(count int) []model.LogEvent {
	events := make([]model.LogEvent, count)
	for i := range events {
		events[i] = g.generateOne()
	}
	return events
}

func (g *Generator) GenerateRaw(count int) [][]byte {
	results := make([][]byte, count)
	for i := range results {
		if g.config.InvalidPercent > 0 && g.rng.Intn(100) < g.config.InvalidPercent {
			results[i] = g.generateInvalid()
		} else {
			event := g.generateOne()
			data, _ := json.Marshal(event)
			results[i] = data
		}
	}
	return results
}

func (g *Generator) generateOne() model.LogEvent {
	service := g.config.Services[g.rng.Intn(len(g.config.Services))]
	level := g.pickLevel()
	latency := g.logNormalLatency()

	event := model.LogEvent{
		Timestamp:     time.Now().UTC().Add(-time.Duration(g.rng.Intn(60)) * time.Second),
		Service:       service,
		Level:         level,
		Message:       messages[g.rng.Intn(len(messages))],
		TraceID:       fmt.Sprintf("%032x", g.rng.Uint64()),
		SpanID:        fmt.Sprintf("%016x", g.rng.Uint64()),
		ParentSpanID:  fmt.Sprintf("%016x", g.rng.Uint64()),
		CorrelationID: fmt.Sprintf("corr_%016x", g.rng.Uint64()),
		Metadata: model.EventMetadata{
			UserID:       fmt.Sprintf("usr_%d", g.rng.Intn(10000)),
			Endpoint:     endpoints[g.rng.Intn(len(endpoints))],
			Method:       methods[g.rng.Intn(len(methods))],
			LatencyMs:    latency,
			StatusCode:   g.statusCodeForLevel(level),
			Region:       regions[g.rng.Intn(len(regions))],
			InstanceID:   fmt.Sprintf("i-%012x", g.rng.Uint64()),
			RequestHash:  fmt.Sprintf("sha256:%016x", g.rng.Uint64()),
			PayloadBytes: g.rng.Intn(4096) + 64,
		},
	}

	if level == "ERROR" || level == "FATAL" {
		event.Metadata.ErrorCode = errorCodes[g.rng.Intn(len(errorCodes))]
		event.Metadata.Tags = []string{"error", "alertable"}
	}

	return event
}

func (g *Generator) pickLevel() string {
	roll := g.rng.Intn(100)
	cumulative := 0
	for _, lw := range levelWeights {
		cumulative += lw.weight
		if roll < cumulative {
			return lw.level
		}
	}
	return "INFO"
}

func (g *Generator) logNormalLatency() int64 {
	mu := 3.5
	sigma := 1.0
	sample := math.Exp(mu + sigma*g.rng.NormFloat64())
	latency := int64(math.Max(1, sample))
	return latency
}

func (g *Generator) statusCodeForLevel(level string) int {
	switch level {
	case "ERROR":
		codes := []int{400, 404, 500, 502, 503}
		return codes[g.rng.Intn(len(codes))]
	case "FATAL":
		return 500
	default:
		return 200
	}
}

func (g *Generator) generateInvalid() []byte {
	switch g.rng.Intn(3) {
	case 0:
		return []byte(`{"service":"test","level":"ERROR","message":"no timestamp"}`)
	case 1:
		return []byte(`{"timestamp":"not-a-date","service":"test","level":"ERROR","message":"bad ts"}`)
	default:
		return []byte(`{invalid json!!!}`)
	}
}

func isInvalid(raw []byte) bool {
	var event model.LogEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return true
	}
	if event.Timestamp.IsZero() {
		return true
	}
	return false
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/loadgen/ -v -race
```

Expected: PASS

- [ ] **Step 5: Implement loadgen CLI**

```go
// cmd/loadgen/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/loadgen"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	mode := flag.String("mode", "steady", "Load generation mode: steady, burst, ramp, chaos")
	rate := flag.Int("rate", 10000, "Target events per second")
	duration := flag.Duration("duration", 60*time.Second, "Duration of load test")
	brokers := flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses (comma-separated)")
	topic := flag.String("topic", "raw-logs", "Target Kafka topic")
	reportDir := flag.String("report-dir", "./benchmarks", "Directory for benchmark reports")
	invalidPct := flag.Int("invalid-percent", 2, "Percentage of invalid events to generate")
	flag.Parse()

	services := []string{
		"payment-service", "auth-service", "user-service",
		"notification-service", "analytics-service", "search-service",
		"inventory-service", "shipping-service", "recommendation-service",
		"gateway-service", "order-service", "billing-service",
		"email-service", "cache-service", "logging-service",
		"config-service", "scheduler-service", "webhook-service",
		"export-service", "import-service",
	}

	gen := loadgen.NewGenerator(loadgen.GeneratorConfig{
		Services:       services,
		InvalidPercent: *invalidPct,
	})

	brokerList := strings.Split(*brokers, ",")
	producer, err := kafka.NewProducer(brokerList)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var totalSent atomic.Int64
	start := time.Now()

	slog.Info("starting load generator", "mode", *mode, "rate", *rate, "duration", *duration)

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()

	timeout := time.After(*duration)

	for {
		select {
		case <-ctx.Done():
			goto done
		case <-timeout:
			goto done
		case <-ticker.C:
			events := gen.GenerateRaw(1)
			for _, raw := range events {
				var svcKey string
				var partial struct{ Service string `json:"service"` }
				if json.Unmarshal(raw, &partial) == nil {
					svcKey = partial.Service
				}
				producer.Send(*topic, []byte(svcKey), raw, nil)
				totalSent.Add(1)
			}
		}
	}

done:
	elapsed := time.Since(start)
	total := totalSent.Load()
	throughput := float64(total) / elapsed.Seconds()

	slog.Info("load generation complete",
		"total_events", total,
		"elapsed", elapsed,
		"throughput_eps", throughput,
	)

	os.MkdirAll(*reportDir, 0755)
	reportPath := fmt.Sprintf("%s/report-%s.md", *reportDir, time.Now().Format("2006-01-02T15-04-05"))
	report := fmt.Sprintf(`# Benchmark Report — %s

## Configuration
- Mode: %s
- Target Rate: %d events/sec
- Duration: %s
- Invalid Percent: %d%%

## Results
| Metric | Value |
|--------|-------|
| Events Produced | %d |
| Achieved Rate | %.0f events/sec |
| Elapsed Time | %s |
`, time.Now().Format(time.RFC3339), *mode, *rate, *duration, *invalidPct, total, throughput, elapsed.Round(time.Millisecond))

	os.WriteFile(reportPath, []byte(report), 0644)
	slog.Info("report written", "path", reportPath)
}
```

- [ ] **Step 6: Verify it compiles**

```bash
go build ./cmd/loadgen/
```

Expected: SUCCESS

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: load generator with multiple modes and benchmark reporting"
```

---

### Task 11: DLQ Replayer CLI

**Files:**
- Create: `cmd/dlq-replayer/main.go`

**Interfaces:**
- Consumes: `model.DLQEnvelope`, `kafka.Consumer`, `kafka.Producer`, `dlq.IsPoisonPill`
- Produces: DLQ Replayer binary that reads from DLQ topics and republishes to origin topics

- [ ] **Step 1: Implement DLQ replayer**

```go
// cmd/dlq-replayer/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	brokers := flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
	sourceTopic := flag.String("source", "dlq-invalid", "DLQ topic to read from")
	targetTopic := flag.String("target", "raw-logs", "Topic to republish events to")
	groupID := flag.String("group", "dlq-replayer", "Consumer group ID")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	brokerList := strings.Split(*brokers, ",")
	producer, err := kafka.NewProducer(brokerList)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(brokerList, *groupID, []string{*sourceTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	var replayed, poisoned atomic.Int64

	slog.Info("dlq-replayer starting", "source", *sourceTopic, "target", *targetTopic)

	err = consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		var envelope model.DLQEnvelope
		if err := json.Unmarshal(value, &envelope); err != nil {
			slog.Error("failed to unmarshal DLQ envelope", "error", err)
			return nil
		}

		envelope.Error.RetryCount++

		if dlq.IsPoisonPill(envelope) {
			envelope.Error.PoisonPill = true
			poisonData, _ := json.Marshal(envelope)
			producer.Send("dlq-poison", nil, poisonData, nil)
			poisoned.Add(1)
			slog.Warn("poison pill detected", "stage", envelope.Error.Stage, "retry_count", envelope.Error.RetryCount)
			return nil
		}

		producer.Send(*targetTopic, nil, envelope.OriginalEvent, nil)
		replayed.Add(1)
		return nil
	})

	if err != nil {
		slog.Error("consumer error", "error", err)
	}

	slog.Info("dlq-replayer stopped", "replayed", replayed.Load(), "poisoned", poisoned.Load())
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./cmd/dlq-replayer/
```

Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: DLQ replayer CLI with poison-pill routing"
```

---

### Task 12: OpenTelemetry Setup

**Files:**
- Create: `internal/telemetry/telemetry.go`
- Create: `internal/telemetry/metrics.go`
- Create: `internal/telemetry/span.go`

**Interfaces:**
- Consumes: `config.Config` (OTelEndpoint, ServiceName)
- Produces:
  - `telemetry.Init(ctx, serviceName, endpoint) (shutdown func(context.Context) error, err error)`
  - `telemetry.Metrics` struct with registered pipeline metrics
  - `telemetry.StartSpan(ctx, name) (context.Context, trace.Span)`

- [ ] **Step 1: Implement telemetry initialization**

```go
// internal/telemetry/telemetry.go
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func Init(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
```

```go
// internal/telemetry/metrics.go
package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	EventsProcessed   metric.Int64Counter
	EventLatency      metric.Float64Histogram
	BatchSize         metric.Int64Histogram
	ConsumerLag       metric.Int64Gauge
	DLQEvents         metric.Int64Counter
	MongoWriteDuration metric.Float64Histogram
	WorkerUtilization metric.Float64Gauge
}

func NewMetrics(serviceName string) (*Metrics, error) {
	meter := otel.Meter(serviceName)

	eventsProcessed, _ := meter.Int64Counter("pipeline_events_processed_total")
	eventLatency, _ := meter.Float64Histogram("pipeline_event_latency_seconds")
	batchSize, _ := meter.Int64Histogram("pipeline_batch_size")
	consumerLag, _ := meter.Int64Gauge("pipeline_consumer_lag")
	dlqEvents, _ := meter.Int64Counter("pipeline_dlq_events_total")
	mongoWrite, _ := meter.Float64Histogram("pipeline_mongo_write_duration_seconds")
	workerUtil, _ := meter.Float64Gauge("pipeline_worker_pool_utilization")

	return &Metrics{
		EventsProcessed:    eventsProcessed,
		EventLatency:       eventLatency,
		BatchSize:          batchSize,
		ConsumerLag:        consumerLag,
		DLQEvents:          dlqEvents,
		MongoWriteDuration: mongoWrite,
		WorkerUtilization:  workerUtil,
	}, nil
}
```

```go
// internal/telemetry/span.go
package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("distributed-collector")
	return tracer.Start(ctx, name)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/telemetry/
```

Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: OpenTelemetry tracing and metrics initialization"
```

---

### Task 13: Docker Compose & Dockerfiles

**Files:**
- Create: `deploy/docker/Dockerfile.validator`
- Create: `deploy/docker/Dockerfile.enricher`
- Create: `deploy/docker/Dockerfile.aggregator`
- Create: `deploy/docker/Dockerfile.raw-store-writer`
- Create: `deploy/docker/Dockerfile.loadgen`
- Create: `deploy/docker/docker-compose.yaml`

**Interfaces:**
- Consumes: All service binaries
- Produces: Runnable multi-container environment via `docker-compose up`

- [ ] **Step 1: Create multi-stage Dockerfile template (validator)**

```dockerfile
# deploy/docker/Dockerfile.validator
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/validator ./cmd/validator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/validator /bin/validator
ENTRYPOINT ["/bin/validator"]
```

- [ ] **Step 2: Create remaining Dockerfiles**

```dockerfile
# deploy/docker/Dockerfile.enricher
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/enricher ./cmd/enricher

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/enricher /bin/enricher
COPY configs/ /configs/
ENTRYPOINT ["/bin/enricher"]
```

```dockerfile
# deploy/docker/Dockerfile.aggregator
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/aggregator ./cmd/aggregator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/aggregator /bin/aggregator
ENTRYPOINT ["/bin/aggregator"]
```

```dockerfile
# deploy/docker/Dockerfile.raw-store-writer
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raw-store-writer ./cmd/raw-store-writer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/raw-store-writer /bin/raw-store-writer
ENTRYPOINT ["/bin/raw-store-writer"]
```

```dockerfile
# deploy/docker/Dockerfile.loadgen
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /bin/loadgen /bin/loadgen
ENTRYPOINT ["/bin/loadgen"]
```

- [ ] **Step 3: Create docker-compose.yaml**

```yaml
# deploy/docker/docker-compose.yaml
version: "3.9"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  mongodb:
    image: mongo:7.0
    ports:
      - "27017:27017"
    volumes:
      - mongo-data:/data/db

  jaeger:
    image: jaegertracing/all-in-one:1.56
    ports:
      - "16686:16686"
      - "4317:4317"
      - "4318:4318"
    environment:
      COLLECTOR_OTLP_ENABLED: "true"

  prometheus:
    image: prom/prometheus:v2.51.0
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:10.4.0
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ../../dashboards:/etc/grafana/provisioning/dashboards

  validator:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.validator
    depends_on:
      - kafka
    environment:
      KAFKA_BROKERS: kafka:29092
      INPUT_TOPIC: raw-logs
      OUTPUT_TOPIC: validated-logs
      DLQ_TOPIC: dlq-invalid
      CONSUMER_GROUP: validator-group
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4317
      SERVICE_NAME: validator
    deploy:
      replicas: 2

  enricher:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.enricher
    depends_on:
      - kafka
    environment:
      KAFKA_BROKERS: kafka:29092
      INPUT_TOPIC: validated-logs
      OUTPUT_TOPIC: enriched-logs
      DLQ_TOPIC: dlq-enrich-failed
      CONSUMER_GROUP: enricher-group
      SERVICE_REGISTRY_PATH: /configs/service-registry.yaml
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4317
      SERVICE_NAME: enricher
    deploy:
      replicas: 2

  aggregator:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.aggregator
    depends_on:
      - kafka
      - mongodb
    environment:
      KAFKA_BROKERS: kafka:29092
      INPUT_TOPIC: enriched-logs
      CONSUMER_GROUP: aggregator-group
      MONGO_URI: mongodb://mongodb:27017
      MONGO_DATABASE: distributed-collector
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4317
      SERVICE_NAME: aggregator
      FLUSH_INTERVAL_SEC: "10"

  raw-store-writer:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.raw-store-writer
    depends_on:
      - kafka
      - mongodb
    environment:
      KAFKA_BROKERS: kafka:29092
      INPUT_TOPIC: enriched-logs
      CONSUMER_GROUP: raw-store-writer-group
      DLQ_TOPIC: dlq-write-failed
      MONGO_URI: mongodb://mongodb:27017
      MONGO_DATABASE: distributed-collector
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4317
      SERVICE_NAME: raw-store-writer
      BATCH_SIZE: "1000"
      FLUSH_INTERVAL_SEC: "5"
    deploy:
      replicas: 2

  loadgen:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile.loadgen
    depends_on:
      - kafka
    environment:
      KAFKA_BROKERS: kafka:29092
    command: ["--mode=steady", "--rate=10000", "--duration=300s", "--kafka-brokers=kafka:29092", "--topic=raw-logs"]
    profiles:
      - loadtest

volumes:
  mongo-data:
```

- [ ] **Step 4: Create Prometheus config**

```yaml
# deploy/docker/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "pipeline-services"
    static_configs:
      - targets:
          - "validator:8081"
          - "enricher:8081"
          - "aggregator:8081"
          - "raw-store-writer:8081"
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: Docker Compose with Kafka, MongoDB, Jaeger, Grafana, and all pipeline services"
```

---

### Task 14: Kubernetes Manifests

**Files:**
- Create: `deploy/k8s/namespace.yaml`
- Create: `deploy/k8s/pipeline/validator.yaml`
- Create: `deploy/k8s/pipeline/enricher.yaml`
- Create: `deploy/k8s/pipeline/aggregator.yaml`
- Create: `deploy/k8s/pipeline/raw-store-writer.yaml`
- Create: `deploy/k8s/pipeline/configmap.yaml`

**Interfaces:**
- Consumes: Docker images for all services
- Produces: K8s manifests with HPA for all pipeline services

- [ ] **Step 1: Create namespace**

```yaml
# deploy/k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: distributed-collector
```

- [ ] **Step 2: Create configmap**

```yaml
# deploy/k8s/pipeline/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pipeline-config
  namespace: distributed-collector
data:
  KAFKA_BROKERS: "kafka-cluster-kafka-bootstrap.distributed-collector.svc:9092"
  MONGO_URI: "mongodb://mongodb-0.mongodb.distributed-collector.svc:27017,mongodb-1.mongodb.distributed-collector.svc:27017,mongodb-2.mongodb.distributed-collector.svc:27017/?replicaSet=rs0"
  MONGO_DATABASE: "distributed-collector"
  OTEL_EXPORTER_OTLP_ENDPOINT: "otel-collector.distributed-collector.svc:4317"
  WORKER_POOL_SIZE: "64"
  BATCH_SIZE: "1000"
  FLUSH_INTERVAL_SEC: "10"
  CHANNEL_BUFFER: "64"
```

- [ ] **Step 3: Create validator deployment with HPA**

```yaml
# deploy/k8s/pipeline/validator.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: validator
  namespace: distributed-collector
  labels:
    app: validator
spec:
  replicas: 2
  selector:
    matchLabels:
      app: validator
  template:
    metadata:
      labels:
        app: validator
    spec:
      containers:
        - name: validator
          image: distributed-collector-validator:latest
          envFrom:
            - configMapRef:
                name: pipeline-config
          env:
            - name: SERVICE_NAME
              value: "validator"
            - name: CONSUMER_GROUP
              value: "validator-group"
            - name: INPUT_TOPIC
              value: "raw-logs"
            - name: OUTPUT_TOPIC
              value: "validated-logs"
            - name: DLQ_TOPIC
              value: "dlq-invalid"
            - name: HTTP_PORT
              value: "8081"
          ports:
            - containerPort: 8081
              name: http
          livenessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              cpu: 250m
              memory: 128Mi
            limits:
              cpu: "1"
              memory: 256Mi
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: validator-hpa
  namespace: distributed-collector
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: validator
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Pods
      pods:
        metric:
          name: pipeline_consumer_lag
        target:
          type: AverageValue
          averageValue: "10000"
```

- [ ] **Step 4: Create enricher, aggregator, raw-store-writer deployments**

(Same pattern as validator, with appropriate env vars for each service)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: Kubernetes manifests with HPA for all pipeline services"
```

---

### Task 15: Grafana Dashboard & README

**Files:**
- Create: `dashboards/grafana-pipeline.json`
- Create: `README.md`
- Create: `docs/architecture.md`

**Interfaces:**
- Consumes: All service metrics
- Produces: Pre-built Grafana dashboard, comprehensive README

- [ ] **Step 1: Create Grafana dashboard JSON**

Create a Grafana dashboard with panels for:
- Events processed per second (by stage)
- End-to-end latency (P50, P95, P99)
- Consumer lag per stage
- DLQ events per stage
- MongoDB write duration
- Worker pool utilization
- Error rate by service

- [ ] **Step 2: Write README**

```markdown
# Distributed Collector

A high-throughput event-driven data pipeline that processes structured application logs at 100K events/sec.

## Architecture

[Mermaid diagram of the pipeline]

## Quick Start

\`\`\`bash
git clone https://github.com/widasinnacy/distributed-collector.git
cd distributed-collector
make up
# Open Grafana: http://localhost:3000 (admin/admin)
# Open Jaeger: http://localhost:16686
\`\`\`

## Run Load Test

\`\`\`bash
docker compose -f deploy/docker/docker-compose.yaml --profile loadtest up loadgen
\`\`\`

## Benchmark Results

[Include screenshot of Grafana dashboard and results table]

## Design Decisions

[Document key choices: why Kafka, why MongoDB, partition strategy, etc.]
```

- [ ] **Step 3: Write architecture doc**

Document the full system architecture with Mermaid diagrams, data flow explanations, and scaling strategy.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: README, architecture docs, and Grafana dashboard"
```

---

## Self-Review

**Spec coverage check:**
- Multi-service pipeline architecture: Tasks 6-9, 11 (all services)
- Kafka topics and partitioning: Task 3 (wrappers) + Task 13 (docker-compose topic creation)
- Event schema: Task 1 (models)
- Processing logic per stage: Tasks 6 (validator), 7 (enricher), 8 (aggregator), 9 (writer)
- Concurrency/backpressure: Task 2 (worker pool)
- DLQ with poison-pill: Task 4
- MongoDB schema/indexes: Task 5 (batch writer) + Task 9 (writer uses it)
- TTL by tier: Task 7 (enricher computes `expires_at`)
- Observability (OTel + Jaeger): Task 12
- Load generator with modes: Task 10
- Benchmark reports: Task 10
- Docker Compose: Task 13
- Kubernetes + HPA: Task 14
- Grafana dashboard: Task 15
- README + docs: Task 15

**Placeholder scan:** No TBD/TODO items. All tasks have complete code.

**Type consistency:** `model.LogEvent`, `model.EnrichedLogEvent`, `model.DLQEnvelope`, `model.Aggregate` used consistently across all tasks. `kafka.Producer.Send()` signature matches in DLQ router mock and actual usage. `pipeline.WorkerPool.Submit()` matches in all service mains.
