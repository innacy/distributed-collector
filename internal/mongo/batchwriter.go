package mongo

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Collection interface {
	InsertMany(ctx context.Context, docs []interface{}) error
}

type BatchConfig struct {
	BatchSize     int
	FlushInterval time.Duration
}

type BatchWriter struct {
	coll   Collection
	config BatchConfig
	buffer []interface{}
	mu     sync.Mutex
	input  chan interface{}
	done   chan struct{}
}

func NewBatchWriter(coll Collection, config BatchConfig) *BatchWriter {
	return &BatchWriter{
		coll:   coll,
		config: config,
		buffer: make([]interface{}, 0, config.BatchSize),
		input:  make(chan interface{}, config.BatchSize*2),
		done:   make(chan struct{}),
	}
}

func (bw *BatchWriter) Start(ctx context.Context) {
	go bw.loop(ctx)
}

func (bw *BatchWriter) loop(ctx context.Context) {
	ticker := time.NewTicker(bw.config.FlushInterval)
	defer ticker.Stop()
	defer close(bw.done)

	for {
		select {
		case doc, ok := <-bw.input:
			if !ok {
				bw.flush(ctx)
				return
			}
			bw.mu.Lock()
			bw.buffer = append(bw.buffer, doc)
			shouldFlush := len(bw.buffer) >= bw.config.BatchSize
			bw.mu.Unlock()

			if shouldFlush {
				bw.flush(ctx)
			}
		case <-ticker.C:
			bw.flush(ctx)
		case <-ctx.Done():
			bw.flush(context.Background())
			return
		}
	}
}

func (bw *BatchWriter) Write(doc interface{}) {
	bw.input <- doc
}

func (bw *BatchWriter) flush(ctx context.Context) {
	bw.mu.Lock()
	if len(bw.buffer) == 0 {
		bw.mu.Unlock()
		return
	}
	batch := bw.buffer
	bw.buffer = make([]interface{}, 0, bw.config.BatchSize)
	bw.mu.Unlock()

	if err := bw.coll.InsertMany(ctx, batch); err != nil {
		slog.Error("batch write failed", "error", err, "batch_size", len(batch))
	}
}

func (bw *BatchWriter) Flush(ctx context.Context) error {
	bw.flush(ctx)
	return nil
}

func (bw *BatchWriter) Stop(ctx context.Context) error {
	close(bw.input)
	<-bw.done
	return nil
}
