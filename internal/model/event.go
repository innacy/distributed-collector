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
