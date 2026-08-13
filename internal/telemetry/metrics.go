package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	EventsProcessed    metric.Int64Counter
	EventLatency       metric.Float64Histogram
	BatchSize          metric.Int64Histogram
	ConsumerLag        metric.Int64Gauge
	DLQEvents          metric.Int64Counter
	MongoWriteDuration metric.Float64Histogram
	WorkerUtilization  metric.Float64Gauge
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
