package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("distributed-collector")
	return tracer.Start(ctx, name)
}
