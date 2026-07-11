package storage

import (
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/wal"
)

// TestStore_ImplementsRawReaderProvider is a regression test for a real production gap:
// store embeds tempodb.Reader as an interface-typed field, which only promotes methods
// declared on tempodb.Reader itself -- it does NOT promote RawReader() (tempodb.
// RawReaderProvider), even though the concrete *readerWriter tempodb.New returns
// implements it. Every frontend sharder (search/structural/metrics) type-asserts its
// reader for this capability at construction; before this fix that assertion silently
// failed for every real deployment, so the frontend's plan-time VCNT-fetch/usage-recording
// codepaths never engaged and every query fell back to full block-sharded dispatch.
func TestStore_ImplementsRawReaderProvider(t *testing.T) {
	tempDir := t.TempDir()

	cfg := Config{
		Trace: tempodb.Config{
			Backend: backend.Local,
			Local: &local.Config{
				Path: path.Join(tempDir, "traces"),
			},
			Block: &common.BlockConfig{
				BloomFP:             .01,
				BloomShardSizeBytes: 100_000,
				Version:             encoding.DefaultEncoding().Version(),
			},
			WAL: &wal.Config{
				Filepath: path.Join(tempDir, "wal"),
			},
			Search: &tempodb.SearchConfig{
				ChunkSizeBytes:  1_000_000,
				ReadBufferCount: 8, ReadBufferSizeBytes: 4 * 1024 * 1024,
			},
		},
	}

	s, err := NewStore(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)

	rrp, ok := s.(tempodb.RawReaderProvider)
	require.True(t, ok, "storage.Store must implement tempodb.RawReaderProvider so frontend sharders' capability check succeeds")
	assert.NotNil(t, rrp.RawReader(), "RawReader() must forward to the underlying Reader's real capability, not return nil")
}
