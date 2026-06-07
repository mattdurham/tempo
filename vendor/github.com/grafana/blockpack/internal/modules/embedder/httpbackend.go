package embedder

import "net/http"

type httpBackend struct {
	client               *http.Client
	serverURL            string
	dim                  int
	maxBatchSize         int
	maxConcurrentBatches int
}
