package dlq

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

type mockProducer struct {
	messages []sentMessage
}

type sentMessage struct {
	topic   string
	key     []byte
	value   []byte
	headers map[string]string
}

func (m *mockProducer) Send(topic string, key, value []byte, headers map[string]string) error {
	m.messages = append(m.messages, sentMessage{topic, key, value, headers})
	return nil
}

func TestRouter_RoutesPermanentErrorToDLQ(t *testing.T) {
	producer := &mockProducer{}
	router := NewRouter(producer, "dlq-invalid", "dlq-poison", "validator", "validator-pod-1")

	event := []byte(`{"service":"test","level":"ERROR"}`)
	err := router.Route(context.Background(), event, "SCHEMA_VIOLATION", "missing timestamp", model.Permanent, 0)
	require.NoError(t, err)

	require.Len(t, producer.messages, 1)
	assert.Equal(t, "dlq-invalid", producer.messages[0].topic)

	var envelope model.DLQEnvelope
	require.NoError(t, json.Unmarshal(producer.messages[0].value, &envelope))
	assert.Equal(t, "validator", envelope.Error.Stage)
	assert.Equal(t, "SCHEMA_VIOLATION", envelope.Error.ErrorType)
	assert.Equal(t, 0, envelope.Error.RetryCount)
	assert.False(t, envelope.Error.PoisonPill)
}

func TestRouter_RoutesPoisonPillToPoisonTopic(t *testing.T) {
	producer := &mockProducer{}
	router := NewRouter(producer, "dlq-invalid", "dlq-poison", "validator", "validator-pod-1")

	event := []byte(`{"service":"test"}`)
	err := router.Route(context.Background(), event, "SCHEMA_VIOLATION", "still broken", model.Permanent, 3)
	require.NoError(t, err)

	require.Len(t, producer.messages, 1)
	assert.Equal(t, "dlq-poison", producer.messages[0].topic)

	var envelope model.DLQEnvelope
	require.NoError(t, json.Unmarshal(producer.messages[0].value, &envelope))
	assert.True(t, envelope.Error.PoisonPill)
}
