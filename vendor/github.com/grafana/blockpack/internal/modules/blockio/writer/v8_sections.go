package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"

	"github.com/klauspost/compress/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// v8SectionWriter accumulates ToCEntries while writing V8 section blobs.

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

	// (1) Block index — always written (v1 and v2).
	if err := w.writeV8BlockIndex(sw); err != nil {
		return err
	}
	// (1b) Column-name bloom index (issue #531) — always written alongside the block
	// index, one fixed-size bloom per block, so readers can prune a block's DATA fetch
	// without any additional I/O beyond what already loading the block index costs.
	if err := w.writeV8ColumnBloomIndex(sw); err != nil {
		return err
	}
	// (2) Per-column range blobs — no-op since #439 (range index removed). The column
	//     sketch blobs (KLL/HLL/TopK) were removed in #435; the value index is authoritative.
	if err := w.writeV8RangeBlobs(sw); err != nil {
		return err
	}
	// NOTE: v1-only sections removed (2026-06-29, v2 lean format now unconditional).
	// writeV8SketchBlobs, writeV8FileSections, writeV8IntrinsicBlobs deleted.
	// Build ToC blob, write it, and write the footer (V8 for v1, V9 for v2).
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

// writeV8ColumnBloomIndex writes the column-name bloom ToCEntry (issue #531).
func (w *Writer) writeV8ColumnBloomIndex(sw *v8SectionWriter) error {
	return sw.writeToCEntry(
		shared.ToCKey{Type: shared.ToCTypeIndex, SubType: shared.ToCSubTypeColumnBloom},
		writeColumnBloomSection(w.blockMetas),
	)
}

// writeV8RangeBlobs is a no-op. The range index was removed in #439.
func (w *Writer) writeV8RangeBlobs(_ *v8SectionWriter) error { return nil }

// NOTE: writeV8FileSections, writeV8SketchBlobs, writeV8IntrinsicBlobs, writeV8ColumnBlobs
// deleted (2026-06-29). The TS index, ColStats, KLL sketch (#435), range index (#439), and
// IntrinsicTOC (#433) sections are gone; the value index is authoritative for pruning.

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
	// V2 lean format unconditional: always FooterV9.
	return writeFooterVersion(sw.out, shared.FooterV9Version, tocOffset, tocLen)
}
