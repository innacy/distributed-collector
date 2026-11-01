package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// InitMetrics installs a global MeterProvider that exports to the default
// Prometheus registry, served by promhttp.Handler() on /metrics.
func InitMetrics() error {
	exporter, err := otelprom.New()
	if err != nil {
		return fmt.Errorf("create prometheus exporter: %w", err)
	}
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)))
	return nil
}
