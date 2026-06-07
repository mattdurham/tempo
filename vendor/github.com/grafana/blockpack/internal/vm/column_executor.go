package vm

// RowSet represents a set of matching row indices
// This interface is implemented by executor.RowSet to avoid import cycles

// ColumnDataProvider is the interface the VM uses to access blockpack data
// This keeps the VM decoupled from the storage implementation

// Metadata operations

// Bulk column scans - return row sets

// ScanEqualAny scans a single column for any of the given values in one pass.
// It is equivalent to unioning N ScanEqual calls but avoids repeating the full
// column scan for each value (dict fast-path for string columns: O(dict+spans)
// instead of N×O(spans)).

// ScanRegexFast accepts a pre-compiled regexp and optional literal prefixes.
// Two calling conventions:
//   - re != nil: standard path. prefixes (if non-nil) are used as a fast-path
//     pre-filter — rows that contain none of the prefixes are skipped before
//     applying the regex. prefixes must be case-sensitive literals.
//   - re == nil: CI fold-contains path. prefixes must be pre-lowercased literals
//     (as produced by AnalyzeRegex for CaseInsensitive patterns). Implementations
//     use strings.ToLower(v) + strings.Contains instead of the regex engine.

// ScanRegexNotMatchFast follows the same re/prefixes calling convention as ScanRegexFast.

// Streaming scans - call callback for each matching row (no RowSet allocation)

// Set operations on row sets

// Individual value access (for operations that can't use bulk scans)
