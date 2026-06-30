// Package benchmark provides utilities for benchmarking blockpack.
// This file anchors private functions to prevent deadcode detection.
package benchmark

// AnchorPrivateFunctions references private helper functions so deadcode
// analysis doesn't report them as unreachable. These are utility functions
// that may be called from external test scripts or future benchmark code.
func AnchorPrivateFunctions() {
	_ = generateEcommerceLikeTraces
	_ = generateEcommerceTrace
	_ = generateEcommerceSpan
}
