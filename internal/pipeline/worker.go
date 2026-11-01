package pipeline

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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

// Instrument records pipeline_events_processed_total{stage,service,status} and
// pipeline_event_latency_seconds for every processed message. Call before Start.
func (wp *WorkerPool) Instrument(stage string) {
	meter := otel.Meter(stage)
	processed, _ := meter.Int64Counter("pipeline_events_processed_total")
	latency, _ := meter.Float64Histogram("pipeline_event_latency_seconds")
	inner := wp.process
	ok := metric.WithAttributes(attribute.String("stage", stage), attribute.String("service", stage), attribute.String("status", "success"))
	fail := metric.WithAttributes(attribute.String("stage", stage), attribute.String("service", stage), attribute.String("status", "error"))
	wp.process = func(ctx context.Context, msg []byte) ([]byte, error) {
		start := time.Now()
		out, err := inner(ctx, msg)
		attrs := ok
		if err != nil {
			attrs = fail
		}
		processed.Add(ctx, 1, attrs)
		latency.Record(ctx, time.Since(start).Seconds(), attrs)
		return out, err
	}
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
	if wp.ctx.Err() != nil {
		return false
	}
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
