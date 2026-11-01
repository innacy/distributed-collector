package kafka

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func init() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

type headerCarrier map[string]string

func (hc headerCarrier) Get(key string) string { return hc[key] }
func (hc headerCarrier) Set(key, value string) {
	hc[key] = value
}
func (hc headerCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range hc {
		keys = append(keys, k)
	}
	return keys
}

func InjectTraceContext(ctx context.Context) map[string]string {
	carrier := make(headerCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func ExtractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	carrier := headerCarrier(headers)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
