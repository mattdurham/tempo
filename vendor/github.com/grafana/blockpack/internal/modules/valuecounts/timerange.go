package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

// TimeRange scans records and returns (minSec, maxSec): the minimum TimeStart and maximum
// TimeEnd across every record. Both bounds are computed via a full O(n) scan; callers must
// NEVER assume sort-order monotonicity on TimeEnd — an earlier-sorted-by-TimeStart record
// can have a later TimeEnd than a later-sorted one (compareRecords sorts by
// (ColumnName, TimeStart, Value, Count); TimeEnd is not a sort key — NOTE-VC-002, issue
// #494). Returns (0, 0) for an empty slice. Shared by valuecountscompactor's mergeLevel
// (this module) and tempo's vcntwriter.go (via the VCNTRecordTimeRange re-export in
// vcnt.go) so this correctness property is proven once, not duplicated across the repo
// boundary.
func TimeRange(records []Record) (minSec, maxSec uint64) {
	if len(records) == 0 {
		return 0, 0
	}
	minSec, maxSec = records[0].TimeStart, records[0].TimeEnd
	for _, r := range records[1:] {
		if r.TimeStart < minSec {
			minSec = r.TimeStart
		}
		if r.TimeEnd > maxSec {
			maxSec = r.TimeEnd
		}
	}
	return minSec, maxSec
}
