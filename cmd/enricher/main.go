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
	"github.com/widasinnacy/distributed-collector/internal/enricher"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "enricher"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "enricher-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "validated-logs"
	}
	if cfg.OutputTopic == "" {
		cfg.OutputTopic = "enriched-logs"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "dlq-enrich-failed"
	}

	registryPath := os.Getenv("SERVICE_REGISTRY_PATH")
	if registryPath == "" {
		registryPath = "configs/service-registry.yaml"
	}

	registry, err := enricher.LoadRegistry(registryPath)
	if err != nil {
		slog.Error("failed to load service registry", "error", err, "path", registryPath)
		os.Exit(1)
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
	dlqRouter := dlq.NewRouter(producer, cfg.DLQTopic, "dlq-poison", "enricher", hostname)

	pipelineVersion := os.Getenv("PIPELINE_VERSION")
	if pipelineVersion == "" {
		pipelineVersion = "1.0.0"
	}

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		var event model.LogEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			dlqRouter.Route(ctx, msg, "UNMARSHAL_ERROR", err.Error(), model.Permanent, 0)
			return nil, err
		}

		info, ok := registry.Lookup(event.Service)
		if !ok {
			dlqRouter.Route(ctx, msg, "UNKNOWN_SERVICE", "service '"+event.Service+"' not in registry", model.Permanent, 0)
			return nil, nil
		}

		enriched := enricher.Enrich(&event, info, pipelineVersion)
		output, _ := json.Marshal(enriched)
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

	slog.Info("enricher service starting", "input", cfg.InputTopic, "output", cfg.OutputTopic)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		pool.Submit(value)
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	consumer.Close()
}
