package vblockpack

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// Encoding implements VersionedEncoding for blockpack format
type Encoding struct{}

// Version returns the version string for this encoding
func (e Encoding) Version() string {
	return VersionString
}

// OpenBlock opens an existing blockpack block for reading
func (e Encoding) OpenBlock(meta *backend.BlockMeta, r backend.Reader) (common.BackendBlock, error) {
	return newBackendBlock(meta, r), nil
}

// NewCompactor creates a new compactor for blockpack blocks
func (e Encoding) NewCompactor(opts common.CompactionOptions) common.Compactor {
	return nil
}

// CreateBlock creates a new blockpack block from an iterator
func (e Encoding) CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta,
	i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {
	return nil, fmt.Errorf("not implemented")
}

// CreateWALBlock creates a new WAL block for writing
func (e Encoding) CreateWALBlock(meta *backend.BlockMeta, filepath, dataEncoding string,
	ingestionSlack time.Duration) (common.WALBlock, error) {
	return nil, fmt.Errorf("not implemented")
}

// OpenWALBlock opens an existing WAL block
func (e Encoding) OpenWALBlock(filename, path string, ingestionSlack, additionalStartSlack time.Duration) (
	common.WALBlock, error, error) {
	return nil, fmt.Errorf("not implemented"), nil
}

// CopyBlock copies a block from one backend to another
func (e Encoding) CopyBlock(ctx context.Context, meta *backend.BlockMeta, from backend.Reader,
	to backend.Writer) error {
	return fmt.Errorf("not implemented")
}

// MigrateBlock migrates a block from another format to blockpack
func (e Encoding) MigrateBlock(ctx context.Context, fromMeta, toMeta *backend.BlockMeta, from backend.Reader,
	to backend.Writer) error {
	return fmt.Errorf("not implemented")
}

// CompactionSupported returns true if compaction is supported for blockpack
func (e Encoding) CompactionSupported() bool {
	return true
}

// WritesSupported returns true if writes are supported for blockpack
func (e Encoding) WritesSupported() bool {
	return true
}

// OwnsWALBlock indicates if this encoding owns the WAL block
func (e Encoding) OwnsWALBlock(entry fs.DirEntry) bool {
	// Check if the entry is a blockpack WAL directory
	return entry.IsDir()
}
