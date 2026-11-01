package mongo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCollection struct {
	mu      sync.Mutex
	batches [][]interface{}
}

func (m *mockCollection) InsertMany(ctx context.Context, docs []interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, docs)
	return nil
}

func (m *mockCollection) getBatches() [][]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batches
}

func TestBatchWriter_FlushesOnSize(t *testing.T) {
	coll := &mockCollection{}
	bw := NewBatchWriter(coll, BatchConfig{
		BatchSize:     5,
		FlushInterval: 10 * time.Second,
	})

	ctx := context.Background()
	bw.Start(ctx)

	for i := 0; i < 5; i++ {
		bw.Write(map[string]string{"id": "event"})
	}

	time.Sleep(100 * time.Millisecond)

	batches := coll.getBatches()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 5)

	bw.Stop(ctx)
}

func TestBatchWriter_FlushesOnInterval(t *testing.T) {
	coll := &mockCollection{}
	bw := NewBatchWriter(coll, BatchConfig{
		BatchSize:     1000,
		FlushInterval: 100 * time.Millisecond,
	})

	ctx := context.Background()
	bw.Start(ctx)

	bw.Write(map[string]string{"id": "event1"})
	bw.Write(map[string]string{"id": "event2"})

	time.Sleep(200 * time.Millisecond)

	batches := coll.getBatches()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 2)

	bw.Stop(ctx)
}

func TestBatchWriter_FlushesOnStop(t *testing.T) {
	coll := &mockCollection{}
	bw := NewBatchWriter(coll, BatchConfig{
		BatchSize:     1000,
		FlushInterval: 10 * time.Second,
	})

	ctx := context.Background()
	bw.Start(ctx)

	bw.Write(map[string]string{"id": "event1"})

	bw.Stop(ctx)

	batches := coll.getBatches()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)
}
