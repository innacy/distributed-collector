package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestHeadersFromSarama(t *testing.T) {
	saramaHeaders := []*sarama.RecordHeader{
		{Key: []byte("traceparent"), Value: []byte("00-abc-def-01")},
		{Key: []byte("custom"), Value: []byte("value")},
	}

	result := headersFromSarama(saramaHeaders)
	assert.Equal(t, "00-abc-def-01", result["traceparent"])
	assert.Equal(t, "value", result["custom"])
}
