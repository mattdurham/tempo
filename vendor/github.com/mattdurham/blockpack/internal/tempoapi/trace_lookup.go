package tempoapi

import (
	"encoding/hex"
	"fmt"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/blockio/reader"
	spanconv "github.com/mattdurham/blockpack/internal/blockio/spanconv"
)

// FindTraceByID looks up a trace by its hex-encoded ID using the trace block index.
// It returns the complete OTLP Trace with spans grouped by resource and scope.
//
// This function uses the trace block index for O(log N) lookup performance.
// If the trace is not found in the file, it returns nil (not an error).
// Returns an error only if the trace ID format is invalid or the file cannot be read.
//
// The traceIDHex parameter is the hex-encoded trace ID (32 hex characters).
// The path parameter specifies the blockpack file to query.
// The storage parameter provides access to the blockpack data.
func FindTraceByID(traceIDHex string, path string, storage Storage) (*tempopb.Trace, error) {
	// Parse hex trace ID to [16]byte
	traceIDBytes, err := hex.DecodeString(traceIDHex)
	if err != nil {
		return nil, fmt.Errorf("invalid trace ID hex format: %w", err)
	}
	if len(traceIDBytes) != 16 {
		return nil, fmt.Errorf("invalid trace ID length: expected 16 bytes, got %d", len(traceIDBytes))
	}

	// Convert to [16]byte array
	var traceID [16]byte
	copy(traceID[:], traceIDBytes)

	// Create reader using storage adapter
	provider, err := storage.GetProvider(path)
	if err != nil {
		return nil, fmt.Errorf("get storage provider: %w", err)
	}

	// Close provider if closeable
	if closeable, ok := provider.(interface{ Close() error }); ok {
		defer func() {
			_ = closeable.Close()
		}()
	}

	rdr, err := reader.NewReaderFromProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}

	// Check if trace block index is available
	if !rdr.HasTraceBlockIndex() {
		return nil, fmt.Errorf("trace index not available in file")
	}

	// Get block IDs containing this trace
	blockIDs := rdr.BlocksForTraceID(traceID)
	if len(blockIDs) == 0 {
		// Trace not found - return nil (not an error)
		return nil, nil
	}

	// Collect matching spans from all blocks
	var allMatches []*spanconv.SpanData

	for _, blockID := range blockIDs {
		// Load the block
		block, err := rdr.GetBlock(blockID) //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("get block %d: %w", blockID, err)
		}

		// Find trace:id column
		traceIDCol := block.GetColumn("trace:id")
		if traceIDCol == nil {
			continue // Block doesn't have trace:id column
		}

		// Scan block for matching trace IDs
		spanCount := block.SpanCount()
		for rowIdx := 0; rowIdx < spanCount; rowIdx++ {
			// Get trace ID for this row
			// trace:id may be stored as bytes or strings depending on writer version
			var rowTraceIDHex string
			if strVal, ok := traceIDCol.StringValue(rowIdx); ok {
				rowTraceIDHex = strVal
			} else if bytesVal, ok := traceIDCol.BytesValue(rowIdx); ok {
				rowTraceIDHex = hex.EncodeToString(bytesVal)
			} else {
				continue
			}

			// Check if it matches our target trace ID
			if rowTraceIDHex != traceIDHex {
				continue
			}

			// Reconstruct full span data from all block columns
			sd, reconstructErr := spanconv.ReconstructSpan(block, rowIdx)
			if reconstructErr != nil {
				return nil, fmt.Errorf("reconstruct span at block %d row %d: %w", blockID, rowIdx, reconstructErr)
			}

			allMatches = append(allMatches, sd)
		}
	}

	// Convert to OTLP format
	trace, err := SpanDataToOTLPTrace(allMatches)
	if err != nil {
		return nil, fmt.Errorf("convert to OTLP: %w", err)
	}

	return trace, nil
}

// Storage is the interface required for trace lookup.
// This matches the public blockpack.Storage interface but is defined here
// to avoid import cycles with the public API package.
type Storage interface {
	// GetProvider returns a ReaderProvider for the given path.
	GetProvider(path string) (blockio.ReaderProvider, error)
}
