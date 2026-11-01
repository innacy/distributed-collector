package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	brokers := flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
	sourceTopic := flag.String("source", "dlq-invalid", "DLQ topic to read from")
	targetTopic := flag.String("target", "raw-logs", "Topic to republish events to")
	groupID := flag.String("group", "dlq-replayer", "Consumer group ID")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	brokerList := strings.Split(*brokers, ",")
	producer, err := kafka.NewProducer(brokerList)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(brokerList, *groupID, []string{*sourceTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	var replayed, poisoned atomic.Int64

	slog.Info("dlq-replayer starting", "source", *sourceTopic, "target", *targetTopic)

	err = consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		var envelope model.DLQEnvelope
		if err := json.Unmarshal(value, &envelope); err != nil {
			slog.Error("failed to unmarshal DLQ envelope", "error", err)
			return nil
		}

		envelope.Error.RetryCount++

		if dlq.IsPoisonPill(envelope) {
			envelope.Error.PoisonPill = true
			poisonData, _ := json.Marshal(envelope)
			producer.Send("dlq-poison", nil, poisonData, nil)
			poisoned.Add(1)
			slog.Warn("poison pill detected", "stage", envelope.Error.Stage, "retry_count", envelope.Error.RetryCount)
			return nil
		}

		producer.Send(*targetTopic, nil, envelope.OriginalEvent, nil)
		replayed.Add(1)
		return nil
	})

	if err != nil {
		slog.Error("consumer error", "error", err)
	}

	slog.Info("dlq-replayer stopped", "replayed", replayed.Load(), "poisoned", poisoned.Load())
}
