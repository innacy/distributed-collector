package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

type Producer struct {
	async sarama.AsyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Flush.Messages = 500
	config.Producer.Flush.Frequency = 100 * 1000000 // 100ms in nanoseconds
	config.Producer.MaxMessageBytes = 1048576

	async, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("new async producer: %w", err)
	}

	return &Producer{async: async}, nil
}

func (p *Producer) Send(topic string, key, value []byte, headers map[string]string) error {
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.ByteEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: headersToSarama(headers),
	}

	p.async.Input() <- msg
	return nil
}

func (p *Producer) Close() error {
	return p.async.Close()
}

func headersToSarama(headers map[string]string) []sarama.RecordHeader {
	result := make([]sarama.RecordHeader, 0, len(headers))
	for k, v := range headers {
		result = append(result, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return result
}

func headersFromSarama(headers []*sarama.RecordHeader) map[string]string {
	result := make(map[string]string, len(headers))
	for _, h := range headers {
		result[string(h.Key)] = string(h.Value)
	}
	return result
}
