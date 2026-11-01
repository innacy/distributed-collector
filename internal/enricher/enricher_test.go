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
