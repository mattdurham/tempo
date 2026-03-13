package writer

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// SPEC-11.5: auto-parsed body columns — parseLogBody detects JSON vs logfmt
// and returns all top-level extracted fields as map[string]string.
// Returns nil on empty body or parse failure (silent-failure policy — NOTE-007).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-logfmt/logfmt"
)

// parseLogBody auto-detects whether body is a JSON object or logfmt string and
// extracts all top-level key-value pairs as string values.
//
// Detection heuristic:
//   - If trimmed body starts with '{', attempt JSON unmarshal into map[string]interface{}.
//   - Otherwise, attempt logfmt decode.
//
// Return value:
//   - Non-nil map with at least one entry on success.
//   - nil on empty body, parse failure, or JSON non-object (e.g., array).
//
// SPEC-11.5: no field cap — all extracted fields are returned.
// NOTE-007: silent failure — caller does nothing on nil return.
func parseLogBody(body string) map[string]string {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}

	if strings.HasPrefix(body, "{") {
		return parseJSONBody(body)
	}
	return parseLogfmtBody(body)
}

// parseJSONBody extracts all top-level key-value pairs from a JSON object.
// Non-string values are formatted via fmt.Sprint. Nested objects/arrays are
// included as their fmt.Sprint representation (not recursed).
// Returns nil if the body is not a valid JSON object.
func parseJSONBody(body string) map[string]string {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		return nil
	}
	if len(obj) == 0 {
		return nil
	}
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		switch s := v.(type) {
		case string:
			out[k] = s
		default:
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}

// parseLogfmtBody extracts all key=value pairs from a logfmt string.
// Returns nil on decode error, if no pairs were extracted, or if no pair
// has an explicit value (plain-text words parse as keys with empty values
// in logfmt — we require at least one key=value pair to consider the body
// structured logfmt).
func parseLogfmtBody(body string) map[string]string {
	// Fast path: require at least one '=' before decoding.
	// Plain text ("this is not structured") has no '=' and cannot be valid key=value logfmt.
	// This also correctly admits "key=" (empty value with explicit equals sign).
	if !strings.ContainsRune(body, '=') {
		return nil
	}
	dec := logfmt.NewDecoder(bytes.NewBufferString(body))
	var out map[string]string
	hasKey := false
	for dec.ScanRecord() {
		for dec.ScanKeyval() {
			if out == nil {
				out = make(map[string]string, 8)
			}
			out[string(dec.Key())] = string(dec.Value())
			hasKey = true
		}
	}
	// Require at least one successfully decoded key (empty values are valid logfmt).
	if dec.Err() != nil || !hasKey {
		return nil
	}
	return out
}
