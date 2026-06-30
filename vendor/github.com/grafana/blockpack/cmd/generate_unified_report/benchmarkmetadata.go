package main

type BenchmarkMetadata struct {
	Timestamp   string     `json:"timestamp"`
	GitHash     string     `json:"gitHash"`
	ResultsHash string     `json:"resultsHash"`
	System      SystemInfo `json:"system"`
}
