package writer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// SPEC-11.5: parseLogBody extracts all top-level fields from JSON/logfmt bodies.

// LOG-01: JSON body extraction — top-level fields become map[string]string entries
func TestParseLogBody_JSON(t *testing.T) {
	result := parseLogBody(`{"level":"info","msg":"started","count":42}`)
	require.Equal(t, "info", result["level"])
	require.Equal(t, "started", result["msg"])
	require.Equal(t, "42", result["count"])
}

// LOG-02: logfmt body extraction — key=value pairs become map entries
func TestParseLogBody_Logfmt(t *testing.T) {
	result := parseLogBody(`level=info msg=started count=42`)
	require.Equal(t, "info", result["level"])
	require.Equal(t, "started", result["msg"])
	require.Equal(t, "42", result["count"])
}

// LOG-03: invalid body (plain text) returns nil — silent failure semantics
func TestParseLogBody_InvalidBody_ReturnsNil(t *testing.T) {
	result := parseLogBody("this is not json or logfmt structured")
	require.Nil(t, result, "expected nil for unparseable body")
}

// LOG-04: empty body returns nil — no columns created
func TestParseLogBody_EmptyBody_ReturnsNil(t *testing.T) {
	result := parseLogBody("")
	require.Nil(t, result)
}

// LOG-05: nested JSON objects are stringified, not recursed
func TestParseLogBody_NestedJSON_OnlyTopLevel(t *testing.T) {
	result := parseLogBody(`{"level":"info","nested":{"a":1}}`)
	require.Equal(t, "info", result["level"])
	// nested object is stringified, not recursed
	require.NotEmpty(t, result["nested"], "nested field should be present as stringified value")
}

// LOG-06: many logfmt fields — all extracted into the result map
func TestParseLogBody_ManyFields_AllExtracted(t *testing.T) {
	body := `level=info service=api region=us-east-1 status=200 latency=42ms`
	result := parseLogBody(body)
	require.Equal(t, "info", result["level"])
	require.Equal(t, "api", result["service"])
	require.Equal(t, "us-east-1", result["region"])
	require.Equal(t, "200", result["status"])
	require.Equal(t, "42ms", result["latency"])
}

// LOG-07: JSON array (not object) returns nil — only top-level objects are parsed
func TestParseLogBody_JSONInvalidNotObject_ReturnsNil(t *testing.T) {
	// JSON array is not a top-level object — silent failure
	result := parseLogBody(`["a","b","c"]`)
	require.Nil(t, result)
}
