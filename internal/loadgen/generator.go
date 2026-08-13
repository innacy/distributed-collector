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
