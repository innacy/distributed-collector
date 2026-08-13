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
