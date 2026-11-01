package dlq

import "github.com/widasinnacy/distributed-collector/internal/model"

func IsPoisonPill(envelope model.DLQEnvelope) bool {
	return envelope.Error.RetryCount >= MaxRetries
}
