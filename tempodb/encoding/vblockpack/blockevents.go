package vblockpack

import (
	"context"
	"log/slog"
	"sync"

	"github.com/grafana/blockpack/blockevents"
)

// Process-level block-event publisher. Mirrors the configuredEmbedURL /
// processEmbedder singleton pattern used elsewhere in this package: configured
// once at startup, read from the block-creation and compaction paths.
//
// When block events are disabled (the default) this holds a NoopPublisher, so
// publishBlockCreated is a zero-cost call on the hot path.
var (
	blockEventPublisher     blockevents.Publisher = blockevents.NewNoopPublisher()
	blockEventPublisherMu   sync.RWMutex
	blockEventPublisherOnce sync.Once
)

// ConfigureBlockEvents installs the process-level block-event publisher from the
// given config. It is safe to call multiple times but only the first call with
// cfg.Enabled actually constructs the Redis-backed publisher; subsequent calls
// are no-ops so the background goroutine and Redis connection are created once.
//
// On construction failure (e.g. missing redis_addr) the publisher is left as the
// default NoopPublisher and the error is logged — block creation/compaction must
// never fail because event publishing could not be set up.
func ConfigureBlockEvents(cfg blockevents.Config) {
	if !cfg.Enabled {
		return
	}
	blockEventPublisherOnce.Do(func() {
		p, err := blockevents.NewPublisher(cfg)
		if err != nil {
			slog.Warn("vblockpack: block-event publisher disabled", "err", err)
			return
		}
		blockEventPublisherMu.Lock()
		blockEventPublisher = p
		blockEventPublisherMu.Unlock()
	})
}

// publishBlockCreated emits a create event for the blockpack object at key.
// It never blocks the caller and never returns an error: a dropped or failed
// event is recoverable downstream (the value index can rebuild from scratch).
func publishBlockCreated(ctx context.Context, objectKey string) {
	blockEventPublisherMu.RLock()
	p := blockEventPublisher
	blockEventPublisherMu.RUnlock()
	_ = p.Publish(ctx, blockevents.Message{Action: blockevents.ActionCreate, Path: objectKey})
}

// blockObjectKey returns the backend object key for a block's data file:
// "<tenant>/<block-id>/data.blockpack". This is the stable identifier the value
// index consumer uses to open the block (tenant + block-id + DataFileName).
func blockObjectKey(tenantID, blockID string) string {
	return tenantID + "/" + blockID + "/" + DataFileName
}
