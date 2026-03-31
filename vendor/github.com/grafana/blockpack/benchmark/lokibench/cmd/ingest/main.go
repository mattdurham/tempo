// Command ingest reads raw Loki chunk files, decodes them into logproto.Stream
// entries, writes to both a chunk store and blockpack, and reports metadata.
//
// Usage:
//
//	go run ./cmd/ingest -chunks /tmp/loki-bench/chunks -out /tmp/loki-bench/prepared -max-mb 2048
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/prometheus/prometheus/model/labels"

	lokibench "github.com/grafana/blockpack/benchmark/lokibench"
)

func main() {
	chunksDir := flag.String("chunks", "/tmp/loki-bench/chunks", "directory containing Loki chunk objects")
	outDir := flag.String("out", "/tmp/loki-bench/prepared", "output directory for prepared stores")
	maxMB := flag.Int("max-mb", 2048, "max MB of chunk data to process")
	tenant := flag.String("tenant", "29", "tenant ID")
	flag.Parse()

	fmt.Printf("=== Loki Chunk Ingest ===\n")
	fmt.Printf("Source:  %s\n", *chunksDir)
	fmt.Printf("Output:  %s\n", *outDir)
	fmt.Printf("Tenant:  %s\n", *tenant)
	fmt.Printf("Limit:   %d MB\n\n", *maxMB)

	// Phase 1: Decode all chunks into streams
	streams, stats := decodeChunks(*chunksDir, *tenant, int64(*maxMB)*1024*1024)

	fmt.Printf("\n=== Decode Results ===\n")
	fmt.Printf("Chunks decoded: %d (failed: %d)\n", stats.decoded, stats.failed)
	fmt.Printf("Total entries:  %d\n", stats.entries)
	fmt.Printf("Unique streams: %d\n", len(streams))
	fmt.Printf("Raw chunk bytes: %.1f MB\n", float64(stats.rawBytes)/1024/1024)
	fmt.Printf("Time range:     %s → %s\n",
		time.Unix(0, stats.minTs).UTC().Format(time.RFC3339),
		time.Unix(0, stats.maxTs).UTC().Format(time.RFC3339))

	// Phase 2: Report stream metadata
	reportMetadata(streams)

	// Phase 3: Write to blockpack
	fmt.Printf("\n=== Writing Blockpack ===\n")
	os.MkdirAll(*outDir, 0o755)
	bpStore, err := lokibench.NewBlockpackStore(*outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create blockpack store: %v\n", err)
		os.Exit(1)
	}

	// Convert map to logproto.Stream slices and write in batches
	var batchStreams []logproto.Stream
	var batchEntries int
	const batchSize = 50000

	for lbls, entries := range streams {
		batchStreams = append(batchStreams, logproto.Stream{
			Labels:  lbls,
			Entries: entries,
		})
		batchEntries += len(entries)
		if batchEntries >= batchSize {
			if err := bpStore.Write(context.Background(), batchStreams); err != nil {
				fmt.Fprintf(os.Stderr, "Blockpack write error: %v\n", err)
				os.Exit(1)
			}
			batchStreams = batchStreams[:0]
			batchEntries = 0
		}
	}
	if len(batchStreams) > 0 {
		if err := bpStore.Write(context.Background(), batchStreams); err != nil {
			fmt.Fprintf(os.Stderr, "Blockpack write error: %v\n", err)
			os.Exit(1)
		}
	}
	if err := bpStore.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Blockpack close error: %v\n", err)
		os.Exit(1)
	}

	bpStat, _ := os.Stat(bpStore.Path())
	fmt.Printf("Blockpack file: %s (%.1f MB)\n", bpStore.Path(), float64(bpStat.Size())/1024/1024)
	fmt.Printf("\nDone. Ready for benchmarking.\n")
}

type decodeStats struct {
	decoded  int
	failed   int
	entries  int
	rawBytes int64
	minTs    int64
	maxTs    int64
}

// decodeChunks reads all chunk files and returns map[labelsString][]Entry.
func decodeChunks(dir, tenant string, maxBytes int64) (map[string][]logproto.Entry, decodeStats) {
	streams := make(map[string][]logproto.Entry)
	var stats decodeStats
	stats.minTs = 1<<62 - 1

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || stats.rawBytes >= maxBytes {
			if stats.rawBytes >= maxBytes {
				return filepath.SkipAll
			}
			return nil
		}

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		stats.rawBytes += int64(len(data))

		parts := strings.Split(path, "/")
		if len(parts) < 2 {
			return nil
		}
		fingerprint := parts[len(parts)-2]
		fname := parts[len(parts)-1]
		chunkKey := fmt.Sprintf("%s/%s/%s", tenant, fingerprint, fname)

		c, decErr := chunk.ParseExternalKey(tenant, chunkKey)
		if decErr != nil {
			stats.failed++
			return nil
		}

		if decErr = c.Decode(chunk.NewDecodeContext(), data); decErr != nil {
			stats.failed++
			return nil
		}

		// After Decode, c.Metric has the stream labels
		lblsStr := c.Metric.String()

		lokiChk := c.Data.(*chunkenc.Facade)
		it, itErr := lokiChk.LokiChunk().Iterator(
			context.Background(),
			time.Unix(0, 0),
			time.Unix(1<<50, 0),
			logproto.FORWARD,
			log.NewNoopPipeline().ForStream(labels.EmptyLabels()),
		)
		if itErr != nil {
			stats.failed++
			return nil
		}

		for it.Next() {
			entry := it.At()
			streams[lblsStr] = append(streams[lblsStr], entry)
			stats.entries++

			ts := entry.Timestamp.UnixNano()
			if ts < stats.minTs {
				stats.minTs = ts
			}
			if ts > stats.maxTs {
				stats.maxTs = ts
			}
		}
		it.Close()

		stats.decoded++
		if stats.decoded%1000 == 0 {
			fmt.Printf("  %d chunks, %d entries, %.0f MB, %d streams\n",
				stats.decoded, stats.entries, float64(stats.rawBytes)/1024/1024, len(streams))
		}

		return nil
	})

	return streams, stats
}

// reportMetadata prints a summary of stream labels and structured metadata.
func reportMetadata(streams map[string][]logproto.Entry) {
	fmt.Printf("\n=== Stream Metadata ===\n")

	// Collect label names and sample values
	labelNames := make(map[string]map[string]struct{}) // name → unique values
	smKeys := make(map[string]map[string]struct{})     // SM key → unique values
	var totalSM int

	for lblsStr, entries := range streams {
		// Parse the Loki labels string using the syntax package
		parsed, err := syntax.ParseLabels(lblsStr)
		if err == nil {
			parsed.Range(func(lbl labels.Label) {
				if labelNames[lbl.Name] == nil {
					labelNames[lbl.Name] = make(map[string]struct{})
				}
				labelNames[lbl.Name][lbl.Value] = struct{}{}
			})
		}

		// Sample structured metadata from first few entries
		for i, entry := range entries {
			if i > 10 {
				break // only sample first entries per stream
			}
			for _, sm := range entry.StructuredMetadata {
				totalSM++
				if smKeys[sm.Name] == nil {
					smKeys[sm.Name] = make(map[string]struct{})
				}
				smKeys[sm.Name][sm.Value] = struct{}{}
			}
		}
	}

	// Print label names sorted by cardinality
	type labelInfo struct {
		name        string
		cardinality int
		samples     []string
	}
	var lblInfos []labelInfo
	for name, vals := range labelNames {
		samples := make([]string, 0, 3)
		for v := range vals {
			if len(samples) < 3 {
				samples = append(samples, v)
			}
		}
		sort.Strings(samples)
		lblInfos = append(lblInfos, labelInfo{name, len(vals), samples})
	}
	sort.Slice(lblInfos, func(i, j int) bool { return lblInfos[i].name < lblInfos[j].name })

	fmt.Printf("\nStream labels (%d unique names):\n", len(lblInfos))
	for _, li := range lblInfos {
		sampleStr := strings.Join(li.samples, ", ")
		if li.cardinality > 3 {
			sampleStr += ", ..."
		}
		fmt.Printf("  %-30s cardinality=%-5d  samples: [%s]\n", li.name, li.cardinality, sampleStr)
	}

	// Print structured metadata keys
	var smInfos []labelInfo
	for name, vals := range smKeys {
		samples := make([]string, 0, 3)
		for v := range vals {
			if len(samples) < 3 {
				samples = append(samples, v)
			}
		}
		sort.Strings(samples)
		smInfos = append(smInfos, labelInfo{name, len(vals), samples})
	}
	sort.Slice(smInfos, func(i, j int) bool { return smInfos[i].name < smInfos[j].name })

	fmt.Printf("\nStructured metadata (%d unique keys, %d total entries sampled):\n", len(smInfos), totalSM)
	for _, si := range smInfos {
		sampleStr := strings.Join(si.samples, ", ")
		if si.cardinality > 3 {
			sampleStr += ", ..."
		}
		fmt.Printf("  %-30s cardinality=%-5d  samples: [%s]\n", si.name, si.cardinality, sampleStr)
	}
}
