package writer

import (
	"fmt"
	"sort"
)

// writer_buffers.go contains functions for buffering and sorting spans.

func (w *Writer) closeCurrentBlock() error {
	if w.current == nil {
		return nil
	}
	blockID := len(w.blockRefs)

	// Track trace-level (resource) columns in dedicated index.
	// This must run before the dedicated loop below so that resource attributes
	// added via b.recordDedicated() are included in the merge.
	for _, trace := range w.current.traces {
		for colName, cb := range trace.resourceAttrs {
			// Auto-detect all resource columns as dedicated
			// Add "resource." prefix to match column naming convention used by SQL compiler
			dedicatedColName := "resource." + colName

			// Get the string value from the trace column (trace columns have 1 row at index 0)
			if len(cb.stringIndexes) > 0 {
				dictIdx := cb.stringIndexes[0]
				if int(dictIdx) < len(cb.stringDictVals) {
					val := cb.stringDictVals[dictIdx]
					key := StringValueKey(val)

					// recordDedicatedValue calls b.recordDedicated() for ColumnTypeString,
					// which is merged into w.dedicatedIndex by the dedicated loop below.
					w.recordDedicatedValue(w.current, dedicatedColName, key)
				}
			}
		}
	}

	// Build dedicated column index for this block.
	// NOTE: This loop MUST run after the trace loop above. The trace loop populates
	// w.current.dedicated via recordDedicatedValue(), and this loop drains
	// w.current.dedicated into w.dedicatedIndex. Reordering these loops would
	// cause resource attribute dedicated values to be silently dropped.
	for col, values := range w.current.dedicated {
		colMap, ok := w.dedicatedIndex[col]
		if !ok {
			colMap = make(map[string]map[int]struct{})
			w.dedicatedIndex[col] = colMap
		}
		for encoded := range values {
			set, ok := colMap[encoded]
			if !ok {
				set = make(map[int]struct{})
			}
			set[blockID] = struct{}{}
			colMap[encoded] = set
		}
	}

	// Build trace block index with span-level indices for this block
	// First invert spanToTrace to get traceIdx -> []spanIdx
	traceSpans := make(map[int][]uint16, len(w.current.traceIndex))
	for spanIdx, traceIdx := range w.current.spanToTrace {
		// CRITICAL: Validate span index before uint16 cast to prevent silent overflow
		if spanIdx > 65535 {
			return fmt.Errorf("span index %d exceeds uint16 maximum (65535)", spanIdx)
		}
		traceSpans[traceIdx] = append(traceSpans[traceIdx], uint16(spanIdx)) //nolint:gosec
	}
	for traceID, traceIdx := range w.current.traceIndex {
		blockEntries, ok := w.traceBlockIndex[traceID]
		if !ok {
			blockEntries = make(map[int][]uint16)
			w.traceBlockIndex[traceID] = blockEntries
		}
		blockEntries[blockID] = traceSpans[traceIdx]
	}

	// Finalize value statistics for this block (v10 feature)
	if w.statsCollector != nil {
		w.current.valueStats = w.statsCollector.finalize()
		// Reset for next block
		w.statsCollector.reset()
	}

	// Streaming mode: serialize and write block to buffered writer.
	// The 64 KB bufio.Writer coalesces writes; Sync() is issued once at
	// the end of flushStreaming() after the buffer is fully flushed.
	if w.outputBuffer != nil {
		payload, idxMeta, err := buildBlockPayload(w.current, w.version, w.zstdEncoder)
		if err != nil {
			return fmt.Errorf("failed to serialize block: %w", err)
		}

		n, err := w.outputBuffer.Write(payload)
		if err != nil {
			return fmt.Errorf("failed to write block: %w", err)
		}
		if n != len(payload) {
			return fmt.Errorf("short write: wrote %d bytes, expected %d", n, len(payload))
		}

		ref := &blockReference{
			offset: w.currentPos,
			length: int64(len(payload)),
			meta:   idxMeta,
		}
		w.blockRefs = append(w.blockRefs, ref)
		w.currentPos += int64(n)

		w.current = nil
		return nil
	}

	// outputBuffer is always set when OutputStream is provided (enforced by NewWriterWithConfig).
	// Reaching here indicates a bug in the caller.
	w.current = nil
	return fmt.Errorf("closeCurrentBlock: outputBuffer is nil (OutputStream was not set)")
}

func (w *Writer) sortSpanBuffer(spans []*BufferedSpan) {
	if w == nil {
		return
	}
	if len(spans) < 2 {
		return
	}

	w.sortServices = w.sortServices[:0]
	for _, span := range spans {
		name := span.ServiceName
		bucket, ok := w.sortBuckets[name]
		if !ok {
			w.sortServices = append(w.sortServices, name)
		}
		bucket = append(bucket, span)
		w.sortBuckets[name] = bucket
	}

	sort.Strings(w.sortServices)

	pos := 0
	for _, name := range w.sortServices {
		bucket := w.sortBuckets[name]
		if len(bucket) > 1 {
			sort.Slice(bucket, func(i, j int) bool {
				if bucket[i].MinHashPrefix[0] != bucket[j].MinHashPrefix[0] {
					return bucket[i].MinHashPrefix[0] < bucket[j].MinHashPrefix[0]
				}
				if bucket[i].MinHashPrefix[1] != bucket[j].MinHashPrefix[1] {
					return bucket[i].MinHashPrefix[1] < bucket[j].MinHashPrefix[1]
				}
				cmp := CompareMinHashSigs(bucket[i].MinHashSig, bucket[j].MinHashSig)
				if cmp != 0 {
					return cmp < 0
				}
				return compareTraceIDs(bucket[i].Span.TraceId, bucket[j].Span.TraceId) < 0
			})
		}
		copy(spans[pos:], bucket)
		pos += len(bucket)
		w.sortBuckets[name] = bucket[:0]
	}
}

// CurrentSize returns an estimated uncompressed size based on spans (~2KB/span).
