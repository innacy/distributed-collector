package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/kafka"
	"github.com/widasinnacy/distributed-collector/internal/loadgen"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	mode := flag.String("mode", "steady", "Load generation mode: steady, burst, ramp, chaos")
	rate := flag.Int("rate", 10000, "Target events per second")
	duration := flag.Duration("duration", 60*time.Second, "Duration of load test")
	brokers := flag.String("kafka-brokers", "localhost:9092", "Kafka broker addresses (comma-separated)")
	topic := flag.String("topic", "raw-logs", "Target Kafka topic")
	reportDir := flag.String("report-dir", "./benchmarks", "Directory for benchmark reports")
	invalidPct := flag.Int("invalid-percent", 2, "Percentage of invalid events to generate")
	flag.Parse()

	services := []string{
		"payment-service", "auth-service", "user-service",
		"notification-service", "analytics-service", "search-service",
		"inventory-service", "shipping-service", "recommendation-service",
		"gateway-service", "order-service", "billing-service",
		"email-service", "cache-service", "logging-service",
		"config-service", "scheduler-service", "webhook-service",
		"export-service", "import-service",
	}

	gen := loadgen.NewGenerator(loadgen.GeneratorConfig{
		Services:       services,
		InvalidPercent: *invalidPct,
	})

	brokerList := strings.Split(*brokers, ",")
	producer, err := kafka.NewProducer(brokerList)
	if err != nil {
		slog.Error("failed to create producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var totalSent atomic.Int64
	start := time.Now()

	slog.Info("starting load generator", "mode", *mode, "rate", *rate, "duration", *duration)

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()

	timeout := time.After(*duration)

	for {
		select {
		case <-ctx.Done():
			goto done
		case <-timeout:
			goto done
		case <-ticker.C:
			events := gen.GenerateRaw(1)
			for _, raw := range events {
				var svcKey string
				var partial struct {
					Service string `json:"service"`
				}
				if json.Unmarshal(raw, &partial) == nil {
					svcKey = partial.Service
				}
				producer.Send(*topic, []byte(svcKey), raw, nil)
				totalSent.Add(1)
			}
		}
	}

done:
	elapsed := time.Since(start)
	total := totalSent.Load()
	throughput := float64(total) / elapsed.Seconds()

	slog.Info("load generation complete",
		"total_events", total,
		"elapsed", elapsed,
		"throughput_eps", throughput,
	)

	os.MkdirAll(*reportDir, 0755)
	reportPath := fmt.Sprintf("%s/report-%s.md", *reportDir, time.Now().Format("2006-01-02T15-04-05"))
	report := fmt.Sprintf(`# Benchmark Report — %s

## Configuration
- Mode: %s
- Target Rate: %d events/sec
- Duration: %s
- Invalid Percent: %d%%

## Results
| Metric | Value |
|--------|-------|
| Events Produced | %d |
| Achieved Rate | %.0f events/sec |
| Elapsed Time | %s |
`, time.Now().Format(time.RFC3339), *mode, *rate, *duration, *invalidPct, total, throughput, elapsed.Round(time.Millisecond))

	os.WriteFile(reportPath, []byte(report), 0644)
	slog.Info("report written", "path", reportPath)
}
