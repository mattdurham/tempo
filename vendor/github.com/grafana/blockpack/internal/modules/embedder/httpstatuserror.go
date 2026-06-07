package embedder

// httpStatusError is returned by doPost when the server returns a non-200 HTTP status.
type httpStatusError struct {
	body   string
	status int
}
