package rw

import "time"

type latencyProvider struct {
	underlying ReaderProvider
	latency    time.Duration
}
