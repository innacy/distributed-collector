package dlq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

func TestIsPoisonPill_UnderThreshold(t *testing.T) {
	envelope := model.DLQEnvelope{
		Error: model.DLQError{RetryCount: 2},
	}
	assert.False(t, IsPoisonPill(envelope))
}

func TestIsPoisonPill_AtThreshold(t *testing.T) {
	envelope := model.DLQEnvelope{
		Error: model.DLQError{RetryCount: 3},
	}
	assert.True(t, IsPoisonPill(envelope))
}

func TestIsPoisonPill_OverThreshold(t *testing.T) {
	envelope := model.DLQEnvelope{
		Error: model.DLQError{RetryCount: 5},
	}
	assert.True(t, IsPoisonPill(envelope))
}
