package pipeline

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/widasinnacy/distributed-collector/internal/telemetry"
)

// Guards the Grafana dashboard: the series it queries must appear on /metrics.
func TestInstrumentedPool_ExposesDashboardMetrics(t *testing.T) {
	require.NoError(t, telemetry.InitMetrics())

	pool := NewWorkerPool(1, 1, func(ctx context.Context, msg []byte) ([]byte, error) { return msg, nil })
	pool.Instrument("validator")
	pool.Start(context.Background())
	require.True(t, pool.Submit([]byte("event")))
	pool.Stop()

	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)

	assert.Regexp(t, `pipeline_events_processed_total\{[^}]*stage="validator"[^}]*status="success"[^}]*\} 1`, string(body))
	assert.Contains(t, string(body), "pipeline_event_latency_seconds_bucket")
}
