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
