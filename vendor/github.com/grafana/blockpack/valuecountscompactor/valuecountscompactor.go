// Package valuecountscompactor is the importable surface for the value-counts
// compactor service.
//
// The implementation lives in internal/modules/valuecountscompactor. Go's
// internal package rule prevents external modules (tempo) from importing that
// package directly, so this thin subpackage re-exports the minimal API needed
// to run the compactor: the Config, the Service, and the Store interface the
// Service depends on.
//
// The compactor periodically merges the many small L0 VCNT (value-counts)
// files written per (tenant, column) into fewer, larger L1+ files, summing
// counts and dropping any group whose net count is <= 0.
package valuecountscompactor

import (
	"github.com/grafana/blockpack/internal/modules/valuecountscompactor"
)

// Config configures the value-counts compactor (the optional
// `value_counts_compactor` YAML block). When Enabled is false the service does
// nothing.
type Config = valuecountscompactor.Config

// Service is the periodic compaction orchestrator.
type Service = valuecountscompactor.Service

// Store is the object-storage surface the compactor needs: list, get, put and
// delete VCNT objects.
type Store = valuecountscompactor.Store

// Object is a key+size pair returned by Store.List.
type Object = valuecountscompactor.Object

// Default configuration values.
const (
	DefaultCompactInterval       = valuecountscompactor.DefaultCompactInterval
	DefaultCompactThresholdFiles = valuecountscompactor.DefaultCompactThresholdFiles
	DefaultCompactBatchBytes     = valuecountscompactor.DefaultCompactBatchBytes
	DefaultMaxRecordsPerMerge    = valuecountscompactor.DefaultMaxRecordsPerMerge
)

// NewService builds a compactor service. cfg.Tenants must be non-empty and the
// store must be non-nil.
func NewService(cfg Config, store Store) (*Service, error) {
	return valuecountscompactor.NewService(cfg, store)
}
