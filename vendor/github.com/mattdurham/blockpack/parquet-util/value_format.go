package parquetutil

import "fmt"

func formatBytesValue(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	if isPrintableASCII(v) {
		return string(v)
	}
	if len(v) > 16 {
		return fmt.Sprintf("%x...", v[:16])
	}
	return fmt.Sprintf("%x", v)
}

func bytesToPrintableString(v []byte) (string, bool) {
	if len(v) == 0 {
		return "", true
	}
	if !isPrintableASCII(v) {
		return "", false
	}
	return string(v), true
}

func isPrintableASCII(v []byte) bool {
	for _, b := range v {
		if b < 32 || b > 126 {
			return false
		}
	}
	return true
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
