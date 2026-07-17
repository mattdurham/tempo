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

// ScanRegexFast accepts a pre-compiled regexp, optional literal prefixes, and a
// RegexFastKind. Two calling conventions, selected by kind:
//   - kind == RegexFastNone: standard path. re must be non-nil. prefixes (if
//     non-nil) are used as a fast-path pre-filter — rows that contain none of the
//     prefixes are skipped before applying the regex. prefixes must be
//     case-sensitive literals.
//   - kind != RegexFastNone (RegexFastContainsCI/CS, RegexFastTrailingChar,
//     RegexFastAnchoredPrefix, RegexFastAnchoredExact): fast-path bypass. re is
//     ignored (may be nil); no regex engine call is made. The shape-specific leaf
//     matcher fully determines the result from prefixes alone. For
//     RegexFastContainsCI, prefixes must be pre-lowercased literals (as produced by
//     AnalyzeRegex for CaseInsensitive patterns) and implementations fold v via
//     strings.ToLower before comparing.

// ScanRegexNotMatchFast follows the same re/prefixes/kind calling convention as ScanRegexFast.

// Streaming scans - call callback for each matching row (no RowSet allocation)

// Set operations on row sets

// Individual value access (for operations that can't use bulk scans)
