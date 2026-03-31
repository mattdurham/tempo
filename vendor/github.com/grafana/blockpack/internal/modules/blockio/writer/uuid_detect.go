package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "encoding/hex"

// isUUID returns true if s is a valid RFC 4122 UUID (36 chars, dashes at positions
// 8, 13, 18, 23; remaining characters are hex digits).
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}

	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHexRune(c) {
				return false
			}
		}
	}

	return true
}

// isHexRune returns true if r is a valid hexadecimal digit (0-9, a-f, A-F).
func isHexRune(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

// uuidToBytes parses a UUID string into its 16-byte binary form by removing dashes
// and hex-decoding the remaining 32 characters. Returns an error if s is not a valid UUID.
func uuidToBytes(s string) ([16]byte, error) {
	var result [16]byte

	if !isUUID(s) {
		return result, &uuidParseError{input: s}
	}

	// Build a 32-char hex string by removing dashes.
	var hexStr [32]byte
	j := 0

	for i := range len(s) {
		if s[i] != '-' {
			hexStr[j] = s[i] //nolint:gosec // safe: isUUID guarantees exactly 32 non-dash chars, so j never exceeds 31
			j++
		}
	}

	decoded, err := hex.DecodeString(string(hexStr[:]))
	if err != nil {
		return result, err
	}

	copy(result[:], decoded)

	return result, nil
}

// shouldStoreAsUUID samples the first min(uuidSampleCount, len(values)) values
// and returns true only if ALL samples are valid UUIDs and at least 1 sample exists.
func shouldStoreAsUUID(values []string) bool {
	n := len(values)
	if n == 0 {
		return false
	}

	if n > uuidSampleCount {
		n = uuidSampleCount
	}

	for i := range n {
		if !isUUID(values[i]) {
			return false
		}
	}

	return true
}

// uuidParseError is the error type returned by uuidToBytes for invalid input.
type uuidParseError struct {
	input string
}

func (e *uuidParseError) Error() string {
	return "invalid UUID: " + e.input
}
