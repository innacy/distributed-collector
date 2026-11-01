package kafka

import (
	"testing"

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
