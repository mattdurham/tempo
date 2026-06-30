package shared

// MakeNoZeroBytes returns a byte slice of length n for use as a decompression
// destination buffer. The caller always fully overwrites the returned bytes
// (e.g. snappy.Decode), so the contents need not be meaningful on return.
func MakeNoZeroBytes(n int) []byte {
	return make([]byte, n)
}
