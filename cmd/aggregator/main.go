package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/aggregator"
	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	mongopkg "github.com/widasinnacy/distributed-collector/internal/mongo"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "aggregator"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "aggregator-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "enriched-logs"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mongoClient, err := mongopkg.Connect(ctx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		slog.Error("failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer mongoClient.Close(ctx)

	engine := aggregator.NewEngine()

	flushInterval := time.Duration(cfg.FlushIntervalSec) * time.Second
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				aggregates := engine.Flush()
				if len(aggregates) == 0 {
					continue
				}
				coll := mongoClient.Collection("aggregates")
				for _, agg := range aggregates {
					filter := map[string]interface{}{
						"service": agg.Service,
						"bucket":  agg.Bucket,
					}
					_ = filter
					_ = coll
					slog.Info("flushed aggregate", "service", agg.Service, "total", agg.Counts.Total)
				}
			case <-ctx.Done():
				aggregates := engine.Flush()
				slog.Info("final flush on shutdown", "aggregates", len(aggregates))
				return
			}
		}
	}()

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		var event model.EnrichedLogEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			slog.Error("failed to unmarshal enriched event", "error", err)
			return nil, err
		}
		engine.Ingest(&event)
		return nil, nil
	})
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("aggregator service starting", "input", cfg.InputTopic, "flush_interval", flushInterval)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		pool.Submit(value)
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	consumer.Close()
}
