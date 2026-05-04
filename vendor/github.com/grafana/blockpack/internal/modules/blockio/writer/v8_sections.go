package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// v8SectionWriter accumulates ToCEntries while writing V8 section blobs.
type v8SectionWriter struct {
	out     *countingWriter
	entries []shared.ToCEntry
}

// writeToCEntry snappy-compresses raw, writes it, and appends a ToCEntry.
func (sw *v8SectionWriter) writeToCEntry(key shared.ToCKey, raw []byte) error {
	compressed := snappy.Encode(nil, raw)
	offset := uint64(sw.out.total) //nolint:gosec
	if _, err := sw.out.Write(compressed); err != nil {
		return fmt.Errorf("writeToCEntry(%v): %w", key, err)
	}
	sw.entries = append(sw.entries, shared.ToCEntry{
		Key:    key,
		Offset: offset,
		Length: uint32(len(compressed)), //nolint:gosec
	})
	return nil
}

// writeV8Sections writes all V8 metadata sections, a unified ToC blob, and Footer V8.
//
// Each section is snappy-compressed and stored as a ToCEntry in the file.
// Intrinsic column blobs are already snappy-compressed and are stored directly
// (not re-compressed) so the reader can decompress them via GetIntrinsicColumnBlob.
func (w *Writer) writeV8Sections() error {
	sw := &v8SectionWriter{out: &w.out}

	// (1) Block index.
	if err := w.writeV8BlockIndex(sw); err != nil {
		return err
	}
	// (2) Per-column range and sketch blobs.
	if err := w.writeV8ColumnBlobs(sw); err != nil {
		return err
	}
	// (3) File-level index and metadata sections.
	if err := w.writeV8FileSections(sw); err != nil {
		return err
	}
	// (4) Per-column intrinsic blobs (already snappy-compressed; do NOT re-compress).
	if err := w.writeV8IntrinsicBlobs(sw); err != nil {
		return err
	}
	// (5) Build ToC blob, write it, and write the V8 footer.
	return w.writeV8ToCAndFooter(sw)
}

// writeV8BlockIndex writes the block index ToCEntry.
func (w *Writer) writeV8BlockIndex(sw *v8SectionWriter) error {
	blockIdxRaw, err := writeBlockIndexSection(nil, w.blockMetas)
	if err != nil {
		return fmt.Errorf("block_index: %w", err)
	}
	return sw.writeToCEntry(
		shared.ToCKey{Type: shared.ToCTypeIndex, SubType: shared.ToCSubTypeBlockIndex},
		blockIdxRaw,
	)
}

// writeV8ColumnBlobs writes per-column range and sketch ToCEntries.
func (w *Writer) writeV8ColumnBlobs(sw *v8SectionWriter) error {
	if err := w.writeV8RangeBlobs(sw); err != nil {
		return err
	}
	return w.writeV8SketchBlobs(sw)
}

// writeV8RangeBlobs writes one ToCEntry per range-indexed column.
func (w *Writer) writeV8RangeBlobs(sw *v8SectionWriter) error {
	colNames := make([]string, 0, len(w.rangeIdx))
	for name, cd := range w.rangeIdx {
		if len(cd.values) > 0 {
			colNames = append(colNames, name)
		}
	}
	slices.Sort(colNames)
	for _, colName := range colNames {
		colBlob, err := writeOneColumnRangeBlob(w.rangeIdx[colName])
		if err != nil {
			return fmt.Errorf("range_index %q: %w", colName, err)
		}
		if len(colBlob) == 0 {
			continue
		}
		if err = sw.writeToCEntry(
			shared.ToCKey{Name: colName, Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeRange},
			colBlob,
		); err != nil {
			return err
		}
	}
	return nil
}

// writeV8SketchBlobs writes one ToCEntry per sketched column.
func (w *Writer) writeV8SketchBlobs(sw *v8SectionWriter) error {
	if len(w.sketchIdx) == 0 {
		return nil
	}
	colSet := make(map[string]struct{}, 64)
	for _, bs := range w.sketchIdx {
		for name := range bs {
			colSet[name] = struct{}{}
		}
	}
	colNames := make([]string, 0, len(colSet))
	for name := range colSet {
		colNames = append(colNames, name)
	}
	slices.Sort(colNames)
	for _, colName := range colNames {
		colBlob, err := writeOneColumnSketchBlob(colName, w.sketchIdx)
		if err != nil {
			return fmt.Errorf("sketch_index %q: %w", colName, err)
		}
		if len(colBlob) == 0 {
			continue
		}
		if err = sw.writeToCEntry(
			shared.ToCKey{Name: colName, Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeSketch},
			colBlob,
		); err != nil {
			return err
		}
	}
	return nil
}

// writeV8FileSections writes trace index, TS index, and optional bloom ToCEntries.
func (w *Writer) writeV8FileSections(sw *v8SectionWriter) error {
	// Trace index.
	var compactBuf bytes.Buffer
	if _, err := writeCompactTraceIndex(&compactBuf, w.blockMetas, w.traceIndex); err != nil {
		return fmt.Errorf("trace_index: %w", err)
	}
	if err := sw.writeToCEntry(
		shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeTrace},
		compactBuf.Bytes(),
	); err != nil {
		return err
	}

	// TS index.
	tsRaw := writeTSIndexSection(w.blockMetas)
	if err := sw.writeToCEntry(
		shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeTS},
		tsRaw,
	); err != nil {
		return err
	}

	// File bloom (optional).
	if len(w.fileBloomSvcNames) == 0 {
		return nil
	}
	bloomRaw, err := writeFileBloomSection(w.fileBloomSvcNames)
	if err != nil {
		return fmt.Errorf("file_bloom: %w", err)
	}
	if len(bloomRaw) == 0 {
		return nil
	}
	return sw.writeToCEntry(
		shared.ToCKey{Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeBloom},
		bloomRaw,
	)
}

// writeV8IntrinsicBlobs writes per-column intrinsic ToCEntries.
// Intrinsic blobs are already snappy-compressed; they are NOT re-compressed via writeToCEntry.
func (w *Writer) writeV8IntrinsicBlobs(sw *v8SectionWriter) error {
	a := w.intrinsicAccum
	if a == nil || a.overCap() {
		return nil
	}
	for _, name := range a.columnNames() {
		blob, err := a.encodeColumn(name)
		if err != nil {
			return fmt.Errorf("intrinsic column %q encode: %w", name, err)
		}
		if len(blob) == 0 {
			continue
		}
		colOffset := uint64(sw.out.total) //nolint:gosec
		if _, writeErr := sw.out.Write(blob); writeErr != nil {
			return fmt.Errorf("intrinsic column %q write: %w", name, writeErr)
		}
		sw.entries = append(sw.entries, shared.ToCEntry{
			Key:    shared.ToCKey{Name: name, Type: shared.ToCTypeMetadata, SubType: shared.ToCSubTypeIntrinsic},
			Offset: colOffset,
			Length: uint32(len(blob)), //nolint:gosec
		})
	}
	return nil
}

// writeV8ToCAndFooter serializes the ToC blob, writes it, and writes the V8 footer.
func (w *Writer) writeV8ToCAndFooter(sw *v8SectionWriter) error {
	signalType := w.signalType
	if signalType == 0 {
		signalType = shared.SignalTypeTrace
	}
	tocSize := shared.ToCBlobHeaderSize
	for _, e := range sw.entries {
		tocSize += e.WireSize()
	}
	tocRaw := make([]byte, 0, tocSize)
	tocRaw = appendUint32LE(tocRaw, uint32(len(sw.entries))) //nolint:gosec
	tocRaw = append(tocRaw, signalType, 0, 0, 0)             // signal_type[1] + reserved[3]
	for _, e := range sw.entries {
		tocRaw = append(tocRaw, e.Marshal()...)
	}

	compressedToC := snappy.Encode(nil, tocRaw)
	tocOffset := uint64(sw.out.total) //nolint:gosec
	if _, err := sw.out.Write(compressedToC); err != nil {
		return fmt.Errorf("toc write: %w", err)
	}
	tocLen := uint32(len(compressedToC)) //nolint:gosec
	return writeFooterV8(sw.out, tocOffset, tocLen)
}
