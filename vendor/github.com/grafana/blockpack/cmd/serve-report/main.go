// Command serve-report starts an HTTP file server for benchmark reports.
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	dir := "benchmark"
	addr := ":10001"

	if _, err := os.Stat(dir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s directory not found. Run from repo root.\n", dir)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           http.FileServer(http.Dir(dir)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	fmt.Printf("==> HTTP server on http://0.0.0.0%s\n", addr)
	fmt.Printf("==> Unified report: http://0.0.0.0%s/unified_report.html\n", addr)
	fmt.Println("==> Press Ctrl+C to stop")

	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
