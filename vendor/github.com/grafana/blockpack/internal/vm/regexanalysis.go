package vm

// SPEC-VM-2: RegexFastKind classifies a regex pattern into a shape that has a
// dedicated row-level leaf matcher, letting compile-time analysis and runtime
// dispatch share a single source of truth instead of stacking re == nil-style
// sentinels.
type RegexFastKind int

const (
	// RegexFastNone means no fast-path shape applies; the caller must fall back
	// to the full regex engine.
	RegexFastNone RegexFastKind = iota
	// RegexFastContainsCI is a case-insensitive pure-contains pattern, e.g. "(?i)bob".
	RegexFastContainsCI
	// RegexFastContainsCS is a case-sensitive pure-contains pattern, e.g. "bob".
	RegexFastContainsCS
	// RegexFastTrailingChar requires at least one character after the literal that
	// is not a newline, e.g. "bob.+".
	RegexFastTrailingChar
	// RegexFastAnchoredPrefix requires the literal at the start of the string, e.g.
	// "^bob" or "^bob.*" (no trailing $).
	RegexFastAnchoredPrefix
	// RegexFastAnchoredExact requires the string to equal the literal exactly, e.g.
	// "^bob$".
	RegexFastAnchoredExact
)

// SPEC-VM-2: RegexAnalysis is a blockpack data type.
type RegexAnalysis struct {
	Prefixes          []string
	CaseInsensitive   bool
	IsLiteralContains bool
	// RequiresTrailingChar is true when the pattern requires at least one
	// non-newline character after the literal (RegexFastKind == RegexFastTrailingChar).
	RequiresTrailingChar bool
	// AnchoredPrefix is true when the pattern requires the literal at the start of
	// the string, with no end anchor (RegexFastKind == RegexFastAnchoredPrefix).
	AnchoredPrefix bool
	// AnchoredExact is true when the pattern requires the string to equal the
	// literal exactly (RegexFastKind == RegexFastAnchoredExact).
	AnchoredExact bool
}
