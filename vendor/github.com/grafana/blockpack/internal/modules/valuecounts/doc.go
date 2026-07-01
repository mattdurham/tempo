// Package valuecounts implements the VCNT value-counts section (issue #400): a per-column
// record of unique values and signed span counts, enabling time-bounded tag-value lookups
// ("what unique values does span:name have in the last 1h?") without scanning spans.
//
// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// # Records
//
// Each Record carries a column name, a [time_start, time_end] window, a canonical value, and
// a signed int64 count. Records are sorted by (ColumnName, TimeStart, Value, Count):
//
//   - positive Count: this value appears in Count spans within the window in this file.
//   - negative Count: delta accounting — a prior file's contribution is being subtracted
//     (source block deleted by retention, or superseded by a compacted output).
//
// # Section wire format (ToCSubTypeValueCounts)
//
// The section is snappy-chunked. Each chunk holds up to ValueCountsRecordsPerChunk records,
// independently snappy-compressed, so a time-bounded lookup decodes only the relevant chunks.
// A chunk directory records each chunk's first (ColumnName, TimeStart) key and byte extent.
//
//	[chunk 0 ][chunk 1 ]...   (concatenated independently-compressed chunk payloads)
//	directory: { MinColumn, MinTimeStart, CompOff, CompLen } per chunk, sorted by key
//
// # Delta accounting
//
// Compact merges records across files, sums Count per (ColumnName, TimeStart, TimeEnd, Value),
// and drops keys whose summed Count is <= 0 — the value no longer exists in any live block.
// The block builder emits positive counts; the compactor additionally emits negated copies of
// the records from each consumed input block so that, after summing, churn is zero for values
// that exist in both input and output.
package valuecounts
