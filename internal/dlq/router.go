package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

const MaxRetries = 3

type MessageSender interface {
	Send(topic string, key, value []byte, headers map[string]string) error
}

type Router struct {
	producer        MessageSender
	dlqTopic        string
	poisonTopic     string
	stage           string
	serviceInstance string
}

func NewRouter(producer MessageSender, dlqTopic, poisonTopic, stage, serviceInstance string) *Router {
	return &Router{
		producer:        producer,
		dlqTopic:        dlqTopic,
		poisonTopic:     poisonTopic,
		stage:           stage,
		serviceInstance: serviceInstance,
	}
}

func (r *Router) Route(ctx context.Context, originalEvent []byte, errorType, errorMsg string, class model.ErrorClass, retryCount int) error {
	isPoisoned := retryCount >= MaxRetries

	envelope := model.DLQEnvelope{
		OriginalEvent: originalEvent,
		Error: model.DLQError{
			Stage:           r.stage,
			ErrorType:       errorType,
			ErrorMessage:    errorMsg,
			FailedAt:        time.Now().UTC(),
			RetryCount:      retryCount,
			PoisonPill:      isPoisoned,
			ServiceInstance: r.serviceInstance,
		},
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal dlq envelope: %w", err)
	}

	topic := r.dlqTopic
	if isPoisoned {
		topic = r.poisonTopic
	}

	return r.producer.Send(topic, nil, data, nil)
}
