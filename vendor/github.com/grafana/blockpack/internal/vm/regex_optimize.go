package vm

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"regexp/syntax"
	"strings"
)

// RegexAnalysis is the result of analyzing a regex pattern for range-index optimization.
// When non-nil, the pattern can be converted to prefix-based range lookups.
type RegexAnalysis struct {
	// Prefixes are the literal prefix strings extracted from the pattern.
	// For simple patterns like "foo.*" this is ["foo"].
	// For alternations like "error|warn" this is ["error", "warn"].
	Prefixes []string

	// CaseInsensitive is true when the pattern uses (?i) flag.
	// The caller must handle case-folding for range lookups.
	CaseInsensitive bool

	// IsLiteralContains is true when the pattern is equivalent to strings.Contains
	// (or strings.Contains(strings.ToLower(v), prefix) for CaseInsensitive).
	// True for pure literals and alternations of literals with no anchors.
	// When true, the regex engine can be bypassed entirely.
	IsLiteralContains bool
}

// RegexPrefixes extracts literal prefixes from a pattern for use as a scan pre-filter.
// Returns nil for case-insensitive patterns (strings.Contains is case-sensitive and
// would produce false negatives if used against lowercased prefixes).
func RegexPrefixes(pattern string) []string {
	a := AnalyzeRegex(pattern)
	if a == nil || a.CaseInsensitive {
		return nil
	}
	return a.Prefixes
}

// AnalyzeRegex examines a regex pattern and returns a RegexAnalysis if the pattern
// can be optimized into prefix-based range-index lookups. Returns nil if the pattern
// is too complex to optimize.
// NOTE-011 (executor/NOTES.md): regex prefix optimization for range-index pruning.
//
// Optimizable patterns:
//   - Literal prefix: "foo.*", "^foo", "^foo.*$", "debug"
//   - Case-insensitive literal: "(?i)debug", "(?i)^error.*"
//   - Alternation of literals: "error|warn|info"
//
// Non-optimizable patterns:
//   - Suffix matches: ".*foo"
//   - Character classes: "[a-z]+"
//   - Complex alternations with wildcards in non-trailing position
func AnalyzeRegex(pattern string) *RegexAnalysis {
	if pattern == "" {
		return nil
	}

	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	parsed = parsed.Simplify()

	var caseInsensitive bool
	prefixes, isLiteralContains := extractPrefixes(parsed, &caseInsensitive)
	if len(prefixes) == 0 {
		return nil
	}

	// Lowercase prefixes for case-insensitive patterns.
	if caseInsensitive {
		for i, p := range prefixes {
			prefixes[i] = strings.ToLower(p)
		}
	}

	return &RegexAnalysis{
		Prefixes:          prefixes,
		CaseInsensitive:   caseInsensitive,
		IsLiteralContains: isLiteralContains,
	}
}

// extractPrefixes walks the parsed regex tree and extracts literal prefixes.
// Returns (prefixes, isLiteralContains): prefixes are the extracted literals;
// isLiteralContains is true when the pattern is equivalent to strings.Contains
// with no anchors. Sets *foldCase to true if any literal node has the FoldCase flag.
func extractPrefixes(re *syntax.Regexp, foldCase *bool) ([]string, bool) {
	switch re.Op {
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase != 0 {
			*foldCase = true
		}
		lit := string(re.Rune)
		if lit == "" {
			return nil, false
		}
		return []string{lit}, true

	case syntax.OpConcat:
		return extractPrefixFromConcat(re.Sub, foldCase)

	case syntax.OpAlternate:
		return extractPrefixFromAlternate(re.Sub, foldCase)

	case syntax.OpCapture:
		if len(re.Sub) == 1 {
			return extractPrefixes(re.Sub[0], foldCase)
		}
		return nil, false

	case syntax.OpBeginText:
		// Bare ^ with nothing after it
		return nil, false

	default:
		return nil, false
	}
}

// extractPrefixFromConcat extracts a literal prefix from a concatenation like:
// BeginText? Literal (DotStar | EndText | empty)*
// Returns (prefixes, isLiteralContains): isLiteralContains is false when anchors are present.
func extractPrefixFromConcat(subs []*syntax.Regexp, foldCase *bool) ([]string, bool) {
	var literalParts []string
	foundLiteral := false
	hasAnchor := false

	for _, sub := range subs {
		switch sub.Op {
		case syntax.OpBeginText, syntax.OpBeginLine:
			// Leading anchor — prefix is extractable but not a pure contains check.
			hasAnchor = true
			continue

		case syntax.OpLiteral:
			if sub.Flags&syntax.FoldCase != 0 {
				*foldCase = true
			}
			lit := string(sub.Rune)
			if lit == "" {
				continue
			}
			literalParts = append(literalParts, lit)
			foundLiteral = true

		case syntax.OpEndText, syntax.OpEndLine:
			// Trailing anchor — still extractable but anchored.
			hasAnchor = true
			continue

		case syntax.OpStar:
			// .* or similar trailing wildcard — everything after is "don't care"
			if foundLiteral {
				return []string{strings.Join(literalParts, "")}, !hasAnchor
			}
			// Leading wildcard (e.g. .*foo) — not optimizable
			return nil, false

		case syntax.OpPlus:
			// .+ — if at start, not optimizable; if after literal, the pattern
			// requires ≥1 additional character, so it is NOT equivalent to strings.Contains.
			if foundLiteral {
				return []string{strings.Join(literalParts, "")}, false
			}
			return nil, false

		case syntax.OpQuest:
			// Optional — stop collecting prefix here; not a pure contains check.
			if foundLiteral {
				return []string{strings.Join(literalParts, "")}, false
			}
			return nil, false

		default:
			// Any other op after literal: return prefix for pre-filtering,
			// but not a pure contains check (unknown match semantics).
			if foundLiteral {
				return []string{strings.Join(literalParts, "")}, false
			}
			return nil, false
		}
	}

	if !foundLiteral {
		return nil, false
	}
	return []string{strings.Join(literalParts, "")}, !hasAnchor
}

// extractPrefixFromAlternate extracts prefixes from each branch of an alternation.
// All branches must be optimizable for the whole alternation to be optimizable.
// isLiteralContains is true only when all branches are isLiteralContains.
func extractPrefixFromAlternate(subs []*syntax.Regexp, foldCase *bool) ([]string, bool) {
	var all []string
	allLiteralContains := true
	for _, sub := range subs {
		prefixes, isLC := extractPrefixes(sub, foldCase)
		if len(prefixes) == 0 {
			return nil, false
		}
		if !isLC {
			allLiteralContains = false
		}
		all = append(all, prefixes...)
	}
	if len(all) == 0 {
		return nil, false
	}
	return all, allLiteralContains
}
