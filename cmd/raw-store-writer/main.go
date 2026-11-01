package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/config"
	"github.com/widasinnacy/distributed-collector/internal/dlq"
	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/model"
	mongopkg "github.com/widasinnacy/distributed-collector/internal/mongo"
	"github.com/widasinnacy/distributed-collector/internal/pipeline"
	"github.com/widasinnacy/distributed-collector/internal/telemetry"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceName = "raw-store-writer"
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "raw-store-writer-group"
	}
	if cfg.InputTopic == "" {
		cfg.InputTopic = "enriched-logs"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "dlq-write-failed"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	mongoClient, err := mongopkg.Connect(ctx, cfg.MongoURI, cfg.MongoDatabase)
	if err != nil {
		slog.Error("failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer mongoClient.Close(ctx)

	coll := mongoClient.Collection("logs")
	batchWriter := mongopkg.NewBatchWriter(&mongoCollectionAdapter{coll: coll}, mongopkg.BatchConfig{
		BatchSize:     cfg.BatchSize,
		FlushInterval: time.Duration(cfg.FlushIntervalSec) * time.Second,
	})
	batchWriter.Start(ctx)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	hostname, _ := os.Hostname()
	dlqRouter := dlq.NewRouter(producer, cfg.DLQTopic, "dlq-poison", "raw-store-writer", hostname)

	pool := pipeline.NewWorkerPool(cfg.WorkerPoolSize, cfg.ChannelBuffer, func(ctx context.Context, msg []byte) ([]byte, error) {
		var event model.EnrichedLogEvent
		if err := json.Unmarshal(msg, &event); err != nil {
			dlqRouter.Route(ctx, msg, "UNMARSHAL_ERROR", err.Error(), model.Permanent, 0)
			return nil, err
		}
		batchWriter.Write(event)
		return nil, nil
	})
	if err := telemetry.InitMetrics(); err != nil {
		slog.Error("failed to init metrics", "error", err)
		os.Exit(1)
	}
	if shutdown, err := telemetry.Init(ctx, cfg.ServiceName, cfg.OTelEndpoint); err != nil {
		slog.Warn("tracing disabled", "error", err)
	} else {
		defer shutdown(context.Background())
	}
	pool.Instrument(cfg.ServiceName)
	pool.Start(ctx)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, []string{cfg.InputTopic})
	if err != nil {
		slog.Error("failed to create consumer", "error", err)
		os.Exit(1)
	}

	svc := pipeline.NewService(cfg.ServiceName, ":"+cfg.HTTPPort)
	go svc.Run(ctx)

	slog.Info("raw-store-writer starting", "input", cfg.InputTopic, "batch_size", cfg.BatchSize)

	if err := consumer.Consume(ctx, func(ctx context.Context, key, value []byte, headers map[string]string) error {
		pool.Submit(value)
		return nil
	}); err != nil {
		slog.Error("consumer error", "error", err)
	}

	pool.Stop()
	batchWriter.Stop(ctx)
	consumer.Close()
}

type mongoCollectionAdapter struct {
	coll *mongo.Collection
}

func (a *mongoCollectionAdapter) InsertMany(ctx context.Context, docs []interface{}) error {
	_, err := a.coll.InsertMany(ctx, docs)
	return err
}
