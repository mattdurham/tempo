package vblockpack

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/grafana/blockpack"
)

// blockObjectKey returns the backend object key for a block's data file:
// "<tenant>/<block-id>/data.blockpack". This is the stable identifier the value
// index write path (NOTE-VI-042) stamps on each indexed (column, span) entry so
// the querier can open the source block from an index hit.
func blockObjectKey(tenantID, blockID string) string {
	return tenantID + "/" + blockID + "/" + DataFileName
}

// parseBlockObjectKey reverses blockObjectKey: "<tenant>/<block-id>/data.blockpack" ->
// (tenant, blockID). Used by readerForSourceRef (plan-d.md D3/DT1) to open a reader for an
// arbitrary SourceRef a structural query's multi-file trace materialization names, which may
// belong to a DIFFERENT block than the one the current Fetch call is for.
func parseBlockObjectKey(key string) (tenantID string, blockID uuid.UUID, err error) {
	suffix := "/" + DataFileName
	if !strings.HasSuffix(key, suffix) {
		return "", uuid.UUID{}, fmt.Errorf("sourceRef %q: missing %q suffix", key, suffix)
	}
	trimmed := strings.TrimSuffix(key, suffix)
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return "", uuid.UUID{}, fmt.Errorf("sourceRef %q: missing tenant/block-id separator", key)
	}
	tenantID = trimmed[:idx]
	blockID, err = uuid.Parse(trimmed[idx+1:])
	if err != nil {
		return "", uuid.UUID{}, fmt.Errorf("sourceRef %q: invalid block id: %w", key, err)
	}
	return tenantID, blockID, nil
}

// readerForSourceRef opens a *blockpack.Reader for an arbitrary SourceRef (an object key of
// exactly the blockObjectKey shape, NOTE-VI-042/NOTE-VI-076) reachable through b's own
// backend.Reader. This generalizes newReader (hardwired to block b's own object) to ANY block in
// the same tenant -- needed by structural queries' multi-file trace materialization (blockpack
// Option A / plan-d.md D3): a trace's spans can be split across sibling compaction-boundary
// blocks, and each span's own SourceRef names which block to read it from. Satisfies
// blockpack.StructuralReaderProvider.
//
// knownSize is deliberately left unset (0): unlike newReaderProvider (which has the CURRENT
// block's own BlockMeta.Size_ on hand), this function only has a SourceRef string -- Size()
// falls back to a StreamReader probe, exactly like tempoReaderProvider.Size already does for the
// no-known-size case.
func (b *blockpackBlock) readerForSourceRef(_ context.Context, sourceRef string) (*blockpack.Reader, error) {
	tenantID, blockID, err := parseBlockObjectKey(sourceRef)
	if err != nil {
		return nil, fmt.Errorf("readerForSourceRef: %w", err)
	}
	fileID := tenantID + "/" + blockID.String()
	provider := &tempoReaderProvider{reader: b.reader, tenantID: tenantID, blockID: blockID}
	return blockpack.NewReaderWithSectionCache(provider, fileID, getCache())
}
