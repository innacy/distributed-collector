package validator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

var validLevels = map[string]bool{
	"DEBUG": true,
	"INFO":  true,
	"WARN":  true,
	"ERROR": true,
	"FATAL": true,
}

type ValidationError struct {
	ErrorType string
	Message   string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorType, e.Message)
}

type logEventRaw struct {
	Timestamp     string              `json:"timestamp"`
	Service       string              `json:"service"`
	Level         string              `json:"level"`
	Message       string              `json:"message"`
	TraceID       string              `json:"trace_id"`
	SpanID        string              `json:"span_id"`
	ParentSpanID  string              `json:"parent_span_id,omitempty"`
	CorrelationID string              `json:"correlation_id,omitempty"`
	Metadata      model.EventMetadata `json:"metadata"`
}

func Validate(raw []byte) (*model.LogEvent, error) {
	var input logEventRaw
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, &ValidationError{ErrorType: "MALFORMED_JSON", Message: err.Error()}
	}

	if input.Timestamp == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "timestamp is required"}
	}

	ts, err := parseTimestamp(input.Timestamp)
	if err != nil {
		return nil, &ValidationError{ErrorType: "INVALID_FORMAT", Message: "timestamp must be valid ISO 8601"}
	}

	if input.Service == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "service is required"}
	}
	if len(input.Service) > 128 {
		return nil, &ValidationError{ErrorType: "INVALID_VALUE", Message: "service must be <= 128 characters"}
	}

	if input.Level == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "level is required"}
	}
	if !validLevels[input.Level] {
		return nil, &ValidationError{ErrorType: "INVALID_VALUE", Message: fmt.Sprintf("level must be one of DEBUG, INFO, WARN, ERROR, FATAL; got %q", input.Level)}
	}

	if input.Message == "" {
		return nil, &ValidationError{ErrorType: "MISSING_FIELD", Message: "message is required"}
	}

	return &model.LogEvent{
		Timestamp:     ts,
		Service:       input.Service,
		Level:         input.Level,
		Message:       input.Message,
		TraceID:       input.TraceID,
		SpanID:        input.SpanID,
		ParentSpanID:  input.ParentSpanID,
		CorrelationID: input.CorrelationID,
		Metadata:      input.Metadata,
	}, nil
}

func parseTimestamp(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
}
