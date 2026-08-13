package enricher

import (
	"time"

	"github.com/widasinnacy/distributed-collector/internal/model"
)

var tierTTL = map[string]time.Duration{
	"critical": 90 * 24 * time.Hour,
	"standard": 30 * 24 * time.Hour,
	"debug":    7 * 24 * time.Hour,
}

func Enrich(event *model.LogEvent, info *ServiceInfo, pipelineVersion string) *model.EnrichedLogEvent {
	now := time.Now().UTC()

	ttl, ok := tierTTL[info.Tier]
	if !ok {
		ttl = tierTTL["standard"]
	}

	return &model.EnrichedLogEvent{
		LogEvent: *event,
		Enrichment: model.Enrichment{
			Team:            info.Team,
			Tier:            info.Tier,
			ProcessedAt:     now,
			HourBucket:      event.Timestamp.UTC().Truncate(time.Hour),
			PipelineVersion: pipelineVersion,
		},
		ExpiresAt:     now.Add(ttl),
		RetentionTier: info.Tier,
		IngestedAt:    now,
	}
}
