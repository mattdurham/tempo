package benchmark

import "time"

type spanTemplate struct {
	service        string
	operation      string
	semanticType   string
	dbSystem       string
	namespace      string
	parentIdx      int
	durationMin    time.Duration
	durationMax    time.Duration
	httpStatusCode int
	isError        bool
}
