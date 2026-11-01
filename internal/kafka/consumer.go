package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type MessageHandler func(ctx context.Context, key, value []byte, headers map[string]string) error

type Consumer struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler MessageHandler
}

func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	return &Consumer{group: group, topics: topics}, nil
}

func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	c.handler = handler
	for {
		if err := c.group.Consume(ctx, c.topics, c); err != nil {
			return fmt.Errorf("consume: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			headers := headersFromSarama(msg.Headers)
			if err := c.handler(session.Context(), msg.Key, msg.Value, headers); err != nil {
				slog.Error("handler error", "error", err, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			}
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
