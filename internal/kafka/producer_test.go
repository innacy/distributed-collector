package kafka

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer_SendMessage(t *testing.T) {
	mockProducer := mocks.NewAsyncProducer(t, nil)
	mockProducer.ExpectInputAndSucceed()

	p := &Producer{
		async: mockProducer,
	}

	err := p.Send("test-topic", []byte("key"), []byte("value"), map[string]string{"traceparent": "00-abc-def-01"})
	require.NoError(t, err)

	mockProducer.Close()
}

// Regression: Return.Successes/Errors were enabled but never read, so the
// producer deadlocked once the result channel buffer (256) filled.
func TestProducer_DoesNotBlockPastResultBuffer(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	mockProducer := mocks.NewAsyncProducer(t, cfg)
	const n = 1000 // > ChannelBufferSize (256)
	for i := 0; i < n; i++ {
		mockProducer.ExpectInputAndSucceed()
	}
	p := wrapProducer(mockProducer)

	done := make(chan struct{})
	go func() {
		for i := 0; i < n; i++ {
			_ = p.Send("t", nil, []byte("v"), nil)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send blocked: producer result channels not drained")
	}
	require.NoError(t, p.Close())
}

func TestProducer_HeadersConvertedCorrectly(t *testing.T) {
	headers := headersToSarama(map[string]string{
		"traceparent": "00-trace-span-01",
		"custom":      "value",
	})

	assert.Len(t, headers, 2)

	found := false
	for _, h := range headers {
		if string(h.Key) == "traceparent" {
			assert.Equal(t, []byte("00-trace-span-01"), h.Value)
			found = true
		}
	}
	assert.True(t, found)
}
