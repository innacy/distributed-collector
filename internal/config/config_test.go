package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"localhost:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "mongodb://localhost:27017", cfg.MongoURI)
	assert.Equal(t, "distributed-collector", cfg.MongoDatabase)
	assert.Equal(t, "localhost:4317", cfg.OTelEndpoint)
	assert.Equal(t, 64, cfg.WorkerPoolSize)
	assert.Equal(t, 1000, cfg.BatchSize)
	assert.Equal(t, 5, cfg.FlushIntervalSec)
}

func TestLoad_FromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("MONGO_URI", "mongodb://prod:27017")
	t.Setenv("WORKER_POOL_SIZE", "128")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "mongodb://prod:27017", cfg.MongoURI)
	assert.Equal(t, 128, cfg.WorkerPoolSize)
}
