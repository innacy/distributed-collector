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

func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.windows = make(map[windowKey]*windowState)
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
