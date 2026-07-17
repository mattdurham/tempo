package vm

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"regexp/syntax"
	"strings"
)

// RegexAnalysis is the result of analyzing a regex pattern for range-index optimization.
// When non-nil, the pattern can be converted to prefix-based range lookups.

// Prefixes are the literal prefix strings extracted from the pattern.
// For simple patterns like "foo.*" this is ["foo"].
// For alternations like "error|warn" this is ["error", "warn"].

// CaseInsensitive is true when the pattern uses (?i) flag.
// The caller must handle case-folding for range lookups.

// IsLiteralContains is true when the pattern is equivalent to strings.Contains
// (or strings.Contains(strings.ToLower(v), prefix) for CaseInsensitive).
// True for pure literals and alternations of literals with no anchors.
// When true, the regex engine can be bypassed entirely.

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

// SPEC-VM-2: AnalyzeRegex examines a regex pattern and returns a RegexAnalysis if the
// pattern can be optimized into prefix-based range-index lookups. Returns nil if the
// pattern is too complex to optimize.
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
	prefixes, kind := extractPrefixes(parsed, &caseInsensitive)
	if len(prefixes) == 0 {
		return nil
	}

	// NOTE-515: Lowercase prefixes for case-insensitive patterns. Case-folding is
	// orthogonal to tail shape: a case-sensitive pure-contains kind is promoted to its
	// case-insensitive counterpart. The trailing-char and anchored leaf matchers
	// (anyPrefixHasTrailingChar, hasAnyPrefix, equalsAny) compare v byte-for-byte and do
	// NOT fold case — CI variants of those three shapes are out of scope for this
	// ticket (no benchmark/property-test coverage for them), so caseInsensitive forces
	// a safe fallback to the real regex engine instead of a silently wrong fast path.
	if caseInsensitive {
		for i, p := range prefixes {
			prefixes[i] = strings.ToLower(p)
		}
		switch kind {
		case RegexFastContainsCS:
			kind = RegexFastContainsCI
		case RegexFastTrailingChar, RegexFastAnchoredPrefix, RegexFastAnchoredExact:
			kind = RegexFastNone
		}
	}

	return &RegexAnalysis{
		Prefixes:             prefixes,
		CaseInsensitive:      caseInsensitive,
		IsLiteralContains:    kind == RegexFastContainsCI || kind == RegexFastContainsCS,
		RequiresTrailingChar: kind == RegexFastTrailingChar,
		AnchoredPrefix:       kind == RegexFastAnchoredPrefix,
		AnchoredExact:        kind == RegexFastAnchoredExact,
	}
}

// extractPrefixes walks the parsed regex tree and extracts literal prefixes.
// Returns (prefixes, kind): prefixes are the extracted literals; kind is the
// RegexFastKind the pattern resolves to (RegexFastNone if it isn't a fast-pathable
// shape — prefixes may still be non-nil in that case, for use as a scan pre-filter).
// Sets *foldCase to true if any literal node has the FoldCase flag.
func extractPrefixes(re *syntax.Regexp, foldCase *bool) ([]string, RegexFastKind) {
	switch re.Op {
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase != 0 {
			*foldCase = true
		}
		lit := string(re.Rune)
		if lit == "" {
			return nil, RegexFastNone
		}
		return []string{lit}, RegexFastContainsCS

	case syntax.OpConcat:
		return extractPrefixFromConcat(re.Sub, foldCase)

	case syntax.OpAlternate:
		return extractPrefixFromAlternate(re.Sub, foldCase)

	case syntax.OpCapture:
		if len(re.Sub) == 1 {
			return extractPrefixes(re.Sub[0], foldCase)
		}
		return nil, RegexFastNone

	case syntax.OpBeginText:
		// Bare ^ with nothing after it
		return nil, RegexFastNone

	default:
		return nil, RegexFastNone
	}
}

// regexTailKind classifies what remains in a concatenation's subs after the leading
// anchor (if any) and the literal run have been consumed — the "tail" of the pattern.
// Kept as its own small enum + classifyTail helper (rather than inlining into
// extractPrefixFromConcat) to keep that function's cyclomatic complexity bounded.
type regexTailKind int

const (
	tailEmpty   regexTailKind = iota // nothing left, e.g. "foo"
	tailStar                         // [OpStar] only, e.g. "foo.*"
	tailStarEnd                      // [OpStar, EndText|EndLine], e.g. "foo.*$"
	tailPlus                         // [OpPlus] only, e.g. "foo.+"
	tailPlusEnd                      // [OpPlus, EndText|EndLine], e.g. "foo.+$"
	tailEndOnly                      // [EndText|EndLine] only, e.g. "foo$"
	tailQuest                        // starts with OpQuest — deferred, e.g. "foo?"
	tailOther                        // anything else — existing fallback, unchanged
)

func isEndAnchor(sub *syntax.Regexp) bool {
	return sub.Op == syntax.OpEndText || sub.Op == syntax.OpEndLine
}

// SPEC-VM-2, NOTE-514: classifyTail resolves subs into one of the regexTailKind
// buckets. subs is whatever remains after the literal run (see
// extractPrefixFromConcat) — it does not re-inspect anything already consumed.
func classifyTail(subs []*syntax.Regexp) regexTailKind {
	if len(subs) == 0 {
		return tailEmpty
	}
	// OpQuest anywhere at the head of the tail is deferred regardless of what follows
	// or precedes it — see .bob/state/plan.md Step 1.1 ("any: tail starts with OpQuest").
	if subs[0].Op == syntax.OpQuest {
		return tailQuest
	}
	if len(subs) == 1 {
		switch subs[0].Op {
		case syntax.OpStar:
			return tailStar
		case syntax.OpPlus:
			return tailPlus
		case syntax.OpEndText, syntax.OpEndLine:
			return tailEndOnly
		default:
			return tailOther
		}
	}
	if len(subs) == 2 && isEndAnchor(subs[1]) {
		switch subs[0].Op {
		case syntax.OpStar:
			return tailStarEnd
		case syntax.OpPlus:
			return tailPlusEnd
		}
	}
	return tailOther
}

// SPEC-VM-2: resolveTailKind maps (leadingAnchor, tail) to a RegexFastKind per the
// .bob/state/plan.md Step 1.1 table. tailQuest and tailOther always fall back to
// RegexFastNone regardless of anchor — both are either deliberately deferred
// (OpQuest) or an unrecognized shape.
func resolveTailKind(leadingAnchor bool, tail regexTailKind) RegexFastKind {
	if tail == tailQuest || tail == tailOther {
		return RegexFastNone
	}
	if leadingAnchor {
		switch tail {
		case tailEmpty, tailStar:
			return RegexFastAnchoredPrefix
		case tailEndOnly:
			return RegexFastAnchoredExact
		default: // tailStarEnd, tailPlus, tailPlusEnd — see plan table, safe fallback
			return RegexFastNone
		}
	}
	switch tail {
	case tailEmpty, tailStar:
		return RegexFastContainsCS
	case tailPlus:
		return RegexFastTrailingChar
	default: // tailStarEnd, tailPlusEnd, tailEndOnly — see plan table, safe fallback
		return RegexFastNone
	}
}

// SPEC-VM-2, NOTE-514: extractPrefixFromConcat extracts a literal prefix from a
// concatenation like:
// (BeginText|BeginLine)? Literal+ (Star|Plus|Quest|EndText|EndLine)?
// Returns (prefixes, kind) resolved from the leading-anchor and tail shape via
// classifyTail/resolveTailKind.
func extractPrefixFromConcat(subs []*syntax.Regexp, foldCase *bool) ([]string, RegexFastKind) {
	i := 0
	leadingAnchor := false
	for i < len(subs) {
		if subs[i].Op != syntax.OpBeginText && subs[i].Op != syntax.OpBeginLine {
			break
		}
		leadingAnchor = true
		i++
	}

	var literalParts []string
	foundLiteral := false
	for i < len(subs) && subs[i].Op == syntax.OpLiteral {
		sub := subs[i]
		if sub.Flags&syntax.FoldCase != 0 {
			*foldCase = true
		}
		lit := string(sub.Rune)
		if lit != "" {
			literalParts = append(literalParts, lit)
			foundLiteral = true
		}
		i++
	}

	if !foundLiteral {
		return nil, RegexFastNone
	}

	kind := resolveTailKind(leadingAnchor, classifyTail(subs[i:]))
	return []string{strings.Join(literalParts, "")}, kind
}

// SPEC-VM-2: extractPrefixFromAlternate extracts prefixes from each branch of an
// alternation. All branches must resolve to the SAME RegexFastKind for the whole alternation to
// resolve to that kind; a mismatch falls back to RegexFastNone (prefixes are still
// returned for pre-filtering, matching the existing fallback behavior). The comparison
// uses each branch's pre-CI-fold kind — case-folding is a top-level concern applied
// once in AnalyzeRegex, not per-branch.
func extractPrefixFromAlternate(subs []*syntax.Regexp, foldCase *bool) ([]string, RegexFastKind) {
	var all []string
	var uniformKind RegexFastKind
	uniform := true
	first := true
	for _, sub := range subs {
		prefixes, kind := extractPrefixes(sub, foldCase)
		if len(prefixes) == 0 {
			return nil, RegexFastNone
		}
		if first {
			uniformKind = kind
			first = false
		} else if kind != uniformKind {
			uniform = false
		}
		all = append(all, prefixes...)
	}
	if len(all) == 0 {
		return nil, RegexFastNone
	}
	if !uniform {
		return all, RegexFastNone
	}
	return all, uniformKind
}
