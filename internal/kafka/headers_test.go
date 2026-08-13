package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraceContext_InjectExtract_Roundtrip(t *testing.T) {
	headers := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	ctx := ExtractTraceContext(context.Background(), headers)
	assert.NotNil(t, ctx)

	injected := InjectTraceContext(ctx)
	assert.Contains(t, injected, "traceparent")
}
