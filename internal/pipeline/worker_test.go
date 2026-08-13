package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool_ProcessesAllItems(t *testing.T) {
	var processed atomic.Int64

	pool := NewWorkerPool(4, 16, func(ctx context.Context, msg []byte) ([]byte, error) {
		processed.Add(1)
		return msg, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	for i := 0; i < 100; i++ {
		require.True(t, pool.Submit([]byte("event")))
	}

	pool.Stop()
	assert.Equal(t, int64(100), processed.Load())
}

func TestWorkerPool_RespectsContext(t *testing.T) {
	pool := NewWorkerPool(2, 4, func(ctx context.Context, msg []byte) ([]byte, error) {
		time.Sleep(100 * time.Millisecond)
		return msg, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	cancel()

	time.Sleep(50 * time.Millisecond)
	assert.False(t, pool.Submit([]byte("should-fail")))
}

func TestWorkerPool_BackpressureWhenFull(t *testing.T) {
	pool := NewWorkerPool(1, 2, func(ctx context.Context, msg []byte) ([]byte, error) {
		time.Sleep(500 * time.Millisecond)
		return msg, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	pool.Submit([]byte("1"))
	pool.Submit([]byte("2"))
	pool.Submit([]byte("3"))

	start := time.Now()
	pool.Submit([]byte("4"))
	elapsed := time.Since(start)

	assert.True(t, elapsed > 400*time.Millisecond, "should block when channel is full")
	pool.Stop()
}
