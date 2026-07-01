package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import "slices"

// Sort sorts records into canonical VCNT order (ColumnName, TimeStart, Value, Count).
// The block builder and compactor both call this before encoding.
func Sort(records []Record) {
	slices.SortFunc(records, func(a, b Record) int {
		return compareRecords(&a, &b)
	})
}

// Compact merges records across files via delta accounting (NOTE-VC-001, issue #400):
//
//   - records are grouped by (ColumnName, TimeStart, TimeEnd, Value)
//   - Count is summed per group
//   - groups whose summed Count is <= 0 are dropped — the value no longer exists in any
//     live block (its positive introduction was canceled by a negative retention delta)
//
// The returned slice is sorted in canonical order. Compact does not mutate the input slice's
// order beyond sorting it in place; callers that need the inputs preserved should copy first.
func Compact(records []Record) []Record {
	if len(records) == 0 {
		return nil
	}
	Sort(records)

	out := records[:0]
	// Walk runs of equal merge-key (records are sorted so equal keys are contiguous, except
	// Count varies within a key — that is exactly the run we sum).
	i := 0
	for i < len(records) {
		k := keyOf(&records[i])
		sum := int64(0)
		j := i
		for j < len(records) && keyOf(&records[j]) == k {
			sum += records[j].Count
			j++
		}
		if sum > 0 {
			out = append(out, Record{
				ColumnName: records[i].ColumnName,
				Value:      records[i].Value,
				TimeStart:  records[i].TimeStart,
				TimeEnd:    records[i].TimeEnd,
				Count:      sum,
			})
		}
		i = j
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// CompactFiles decodes every (data, dir) pair, concatenates the records, and runs Compact.
// It is the file-level entry point used by the periodic vcnt compactor: read all .vcnt files
// for a tenant, merge by key, sum counts, drop dead values.
func CompactFiles(files []DecodedFile) ([]Record, error) {
	var all []Record
	for i := range files {
		recs, err := DecodeAll(files[i].Data, files[i].Dir)
		if err != nil {
			return nil, err
		}
		all = append(all, recs...)
	}
	return Compact(all), nil
}

// DecodedFile pairs a VCNT section's raw chunk bytes with its chunk directory.
type DecodedFile struct {
	Data []byte
	Dir  []ChunkDirEntry
}
