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

func TestEngine_Reset(t *testing.T) {
	engine := NewEngine()

	engine.Ingest(makeEvent("svc-a", "INFO", 10, "/api", ""))
	engine.Reset()

	aggregates := engine.Flush()
	assert.Empty(t, aggregates)
}

func TestEngine_FlushResetsWindows(t *testing.T) {
	engine := NewEngine()

	engine.Ingest(makeEvent("svc-a", "INFO", 10, "/api", ""))
	first := engine.Flush()
	require.Len(t, first, 1)

	second := engine.Flush()
	assert.Empty(t, second)
}

func TestEngine_MinuteBucket(t *testing.T) {
	engine := NewEngine()

	engine.Ingest(makeEvent("svc-a", "INFO", 10, "/api", ""))

	aggregates := engine.Flush()
	require.Len(t, aggregates, 1)
	assert.Equal(t, time.Date(2026, 8, 13, 14, 30, 0, 0, time.UTC), aggregates[0].Bucket)
	assert.Equal(t, 60, aggregates[0].WindowSeconds)
}
