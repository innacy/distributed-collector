package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_ValidEvent(t *testing.T) {
	raw := []byte(`{
		"timestamp": "2026-08-13T12:30:00.123Z",
		"service": "payment-service",
		"level": "ERROR",
		"message": "failed to process transaction",
		"trace_id": "abc123",
		"span_id": "def456",
		"metadata": {"latency_ms": 342}
	}`)

	event, err := Validate(raw)
	require.NoError(t, err)
	assert.Equal(t, "payment-service", event.Service)
	assert.Equal(t, "ERROR", event.Level)
	assert.Equal(t, int64(342), event.Metadata.LatencyMs)
}

func TestValidate_MissingTimestamp(t *testing.T) {
	raw := []byte(`{
		"service": "payment-service",
		"level": "ERROR",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "MISSING_FIELD", ve.ErrorType)
	assert.Contains(t, ve.Message, "timestamp")
}

func TestValidate_InvalidLevel(t *testing.T) {
	raw := []byte(`{
		"timestamp": "2026-08-13T12:30:00Z",
		"service": "test",
		"level": "UNKNOWN",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "INVALID_VALUE", ve.ErrorType)
}

func TestValidate_InvalidTimestamp(t *testing.T) {
	raw := []byte(`{
		"timestamp": "not-a-date",
		"service": "test",
		"level": "INFO",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "INVALID_FORMAT", ve.ErrorType)
}

func TestValidate_ServiceTooLong(t *testing.T) {
	longService := make([]byte, 129)
	for i := range longService {
		longService[i] = 'a'
	}

	raw := []byte(`{
		"timestamp": "2026-08-13T12:30:00Z",
		"service": "` + string(longService) + `",
		"level": "INFO",
		"message": "test"
	}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "INVALID_VALUE", ve.ErrorType)
}

func TestValidate_MalformedJSON(t *testing.T) {
	raw := []byte(`{not valid json}`)

	_, err := Validate(raw)
	require.Error(t, err)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, "MALFORMED_JSON", ve.ErrorType)
}
