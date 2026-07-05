// Package valueindexcompactor is the importable surface for the value-index
// compactor service.
//
// The implementation lives in internal/modules/valueindexcompactor. Go's
// internal package rule prevents external modules (tempo) from importing that
// package directly, so this thin subpackage re-exports the minimal API needed
// to run the compactor: the Config, the Service, and the IndexStore/
// SourceExister interfaces the Service depends on.
//
// The compactor is the third stage of the value-index pipeline (publisher #397,
// consumer #398, compactor #399): it periodically merges the many small L0
// value index files written by the consumer into fewer, larger L1/L2 files per
// (tenant, column), dropping entries whose source blockpack has been deleted by
// retention. NOTE-VI-017.
package valueindexcompactor

import (
	"github.com/grafana/blockpack/internal/modules/valueindexcompactor"
)

// Config configures the value-index compactor (the optional
// `value_index_compactor` YAML block). When Enabled is false the service does
// nothing.
type Config = valueindexcompactor.Config

// Service is the periodic compaction orchestrator.
type Service = valueindexcompactor.Service

// IndexStore is the object-storage surface the compactor needs: list, get, put
// and delete value index objects.
type IndexStore = valueindexcompactor.IndexStore

// SourceExister reports whether a source blockpack object still exists. A
// retention-deleted blockpack returns false, causing its entries to be dropped
// during compaction.
type SourceExister = valueindexcompactor.SourceExister

// Default configuration values.
const (
	DefaultIndexPrefix           = valueindexcompactor.DefaultIndexPrefix
	DefaultCompactInterval       = valueindexcompactor.DefaultCompactInterval
	DefaultCompactThresholdFiles = valueindexcompactor.DefaultCompactThresholdFiles
	DefaultMaxOutputBytes        = valueindexcompactor.DefaultMaxOutputBytes
)

// NewService builds a compactor service. cfg.Tenants must be non-empty and the
// store must be non-nil. exister may be nil to skip the retention check.
func NewService(cfg Config, store IndexStore, exister SourceExister) (*Service, error) {
	return valueindexcompactor.NewService(cfg, store, exister)
}

// IndexObject is a key+size pair returned by IndexStore.List.
type IndexObject = valueindexcompactor.IndexObject
