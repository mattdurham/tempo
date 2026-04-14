package vm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeRegex_LiteralPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		prefixes []string
	}{
		{"simple prefix with dot-star", "foo.*", []string{"foo"}},
		{"anchored prefix", "^foo", []string{"foo"}},
		{"anchored prefix with dot-star", "^foo.*", []string{"foo"}},
		{"anchored prefix with dot-star and end anchor", "^foo.*$", []string{"foo"}},
		{"exact literal no metachar", "debug", []string{"debug"}},
		{"literal with escaped dot", `foo\.bar`, []string{"foo.bar"}},
		{"prefix ending with dot-star-dollar", "GET /api.*$", []string{"GET /api"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := AnalyzeRegex(tt.pattern)
			require.NotNil(t, result, "expected analysis for %q", tt.pattern)
			assert.False(t, result.CaseInsensitive)
			assert.Equal(t, tt.prefixes, result.Prefixes)
		})
	}
}

func TestAnalyzeRegex_CaseInsensitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		prefixes []string
	}{
		{"case-insensitive literal", "(?i)debug", []string{"debug"}},
		{"case-insensitive prefix", "(?i)^error.*", []string{"error"}},
		{"case-insensitive with flags group", "(?i)warn", []string{"warn"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := AnalyzeRegex(tt.pattern)
			require.NotNil(t, result, "expected analysis for %q", tt.pattern)
			assert.True(t, result.CaseInsensitive)
			assert.Equal(t, tt.prefixes, result.Prefixes)
		})
	}
}

func TestAnalyzeRegex_Alternation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		prefixes []string
	}{
		{"simple alternation", "error|warn|info", []string{"error", "warn", "info"}},
		{"alternation with anchors", "^error|^warn", []string{"error", "warn"}},
		{"alternation with dot-star", "error.*|warn.*", []string{"error", "warn"}},
		{"alternation with dot in branches", "f.o|b.r", []string{"f", "b"}},
		// Simplify() factors "foo(?:bar|baz)qux" → concat("fooba", alt("r","z"), "qux").
		// The shared prefix "fooba" becomes a literal node; extraction stops at the alternation.
		{"common prefix factored by simplify", "foo(?:bar|baz)qux", []string{"fooba"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := AnalyzeRegex(tt.pattern)
			require.NotNil(t, result, "expected analysis for %q", tt.pattern)
			assert.Equal(t, tt.prefixes, result.Prefixes)
		})
	}
}

func TestAnalyzeRegex_IsLiteralContains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		pattern             string
		wantLC              bool
		wantCaseInsensitive bool
	}{
		// Pure literals → true (no anchors, no trailing ops)
		{"bare literal", "error", true, false},
		{"ci bare literal", "(?i)error", true, true},
		// Trailing .* with no anchor → true (strings.Contains semantics)
		{"prefix dot-star", "error.*", true, false},
		{"ci prefix dot-star", "(?i)error.*", true, true},
		// Anchored patterns → false (not a pure contains check)
		{"anchored prefix", "^error", false, false},
		{"anchored dot-star", "^error.*", false, false},
		{"ci anchored dot-star", "(?i)^error.*", false, true},
		// OpPlus after literal → false (.+ requires ≥1 extra char, not pure contains)
		{"prefix dot-plus", "error.+", false, false},
		{"ci prefix dot-plus", "(?i)error.+", false, true},
		// Character class (default op) after literal → false
		{"ci prefix char-class", "(?i)error[0-9]", false, true},
		// Alternation of pure literals → true
		{"alternation literals", "error|warn", true, false},
		{"ci alternation literals", "(?i)error|warn", true, true},
		// Alternation with anchored branch → false
		{"alternation anchored branch", "^error|warn", false, false},
		// Alternation with dot-star branches → true
		{"alternation dot-star branches", "error.*|warn.*", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := AnalyzeRegex(tt.pattern)
			require.NotNil(t, result, "expected analysis for %q", tt.pattern)
			assert.Equal(t, tt.wantLC, result.IsLiteralContains,
				"IsLiteralContains mismatch for %q", tt.pattern)
			assert.Equal(t, tt.wantCaseInsensitive, result.CaseInsensitive,
				"CaseInsensitive mismatch for %q", tt.pattern)
		})
	}
}

func TestAnalyzeRegex_NotOptimizable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
	}{
		{"suffix match", ".*foo"},
		{"character class", "[a-z]+"},
		{"dot-plus prefix", ".+foo"},
		{"empty pattern", ""},
		{"just dot-star", ".*"},
		{"just anchors", "^$"},
		{"backref", `(foo)\1`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := AnalyzeRegex(tt.pattern)
			assert.Nil(t, result, "expected nil for non-optimizable pattern %q, got %+v", tt.pattern, result)
		})
	}
}
