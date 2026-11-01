package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
	"github.com/widasinnacy/distributed-collector/internal/validator"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "validator"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "validator-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "raw-logs"
	}
	if cfg.OutputTopic == "" {
		cfg.OutputTopic = "validated-logs"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "dlq-invalid"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	hostname, _ := os.Hostname()
	dlqRouter := dlq.NewRouter(producer, cfg.DLQTopic, "dlq-poison", "validator", hostname)

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		event, err := validator.Validate(msg)
		if err != nil {
			var ve *validator.ValidationError
			if ok := isValidationError(err, &ve); ok {
				dlqRouter.Route(ctx, msg, ve.ErrorType, ve.Message, model.Permanent, 0)
			}
			return nil, err
		}

		output, _ := json.Marshal(event)
		headers := kafka.InjectTraceContext(ctx)
		producer.Send(cfg.OutputTopic, []byte(event.Service), output, headers)
		return output, nil
	})
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("validator service starting", "input", cfg.InputTopic, "output", cfg.OutputTopic)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		traceCtx := kafka.ExtractTraceContext(ctx, headers)
		pool.Submit(value)
		_ = traceCtx
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	consumer.Close()
}

func isValidationError(err error, target **validator.ValidationError) bool {
	ve, ok := err.(*validator.ValidationError)
	if ok {
		*target = ve
	}
	return ok
}
