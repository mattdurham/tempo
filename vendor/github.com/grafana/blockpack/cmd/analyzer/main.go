// Package main runs the anyloop analyzer as a standalone tool.
// Usage: go run ./cmd/analyzer -- ./internal/...
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/grafana/blockpack/cmd/analyzer/anyloop"
)

func main() {
	singlechecker.Main(anyloop.Analyzer)
}
