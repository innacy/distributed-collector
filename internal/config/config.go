package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers    []string
	MongoURI        string
	MongoDatabase   string
	OTelEndpoint    string
	ServiceName     string
	WorkerPoolSize  int
	BatchSize       int
	FlushIntervalSec int
	ChannelBuffer   int
	ConsumerGroup   string
	InputTopic      string
	OutputTopic     string
	DLQTopic        string
	HTTPPort        string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:    strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ","),
		MongoURI:        envOrDefault("MONGO_URI", "mongodb://localhost:27017"),
		MongoDatabase:   envOrDefault("MONGO_DATABASE", "distributed-collector"),
		OTelEndpoint:    envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		ServiceName:     envOrDefault("SERVICE_NAME", "unknown"),
		WorkerPoolSize:  envOrDefaultInt("WORKER_POOL_SIZE", 64),
		BatchSize:       envOrDefaultInt("BATCH_SIZE", 1000),
		FlushIntervalSec: envOrDefaultInt("FLUSH_INTERVAL_SEC", 5),
		ChannelBuffer:   envOrDefaultInt("CHANNEL_BUFFER", 64),
		ConsumerGroup:   envOrDefault("CONSUMER_GROUP", ""),
		InputTopic:      envOrDefault("INPUT_TOPIC", ""),
		OutputTopic:     envOrDefault("OUTPUT_TOPIC", ""),
		DLQTopic:        envOrDefault("DLQ_TOPIC", ""),
		HTTPPort:        envOrDefault("HTTP_PORT", "8081"),
	}
	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
