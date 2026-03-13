package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/grafana/blockpack"
)

// fileProvider implements blockpack.ReaderProvider for a local file.
type fileProvider struct {
	f    *os.File
	size int64
}

func (p *fileProvider) Size() (int64, error) { return p.size, nil }
func (p *fileProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.f.ReadAt(buf, off)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: bpanalyze <file.blockpack>\n")
		os.Exit(1)
	}

	path := os.Args[1]
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat: %v\n", err)
		os.Exit(1)
	}

	r, err := blockpack.NewReaderFromProvider(&fileProvider{f: f, size: fi.Size()})
	if err != nil {
		fmt.Fprintf(os.Stderr, "reader: %v\n", err)
		os.Exit(1)
	}

	report, err := blockpack.AnalyzeFileLayout(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "analyze: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("File: %s\n", path)
	fmt.Printf("Size: %.2f MB\n", float64(report.FileSize)/(1024*1024))
	fmt.Printf("Version: %d\n", report.FileVersion)
	fmt.Printf("Blocks: %d\n", report.BlockCount)
	fmt.Printf("Total Spans: %d\n", report.TotalSpans)
	fmt.Println()

	// Aggregate by section type.
	type sectionAgg struct {
		name              string
		compressedBytes   int64
		uncompressedBytes int64
		count             int
	}

	agg := map[string]*sectionAgg{}
	for _, s := range report.Sections {
		cat := categorize(s.Section)
		a, ok := agg[cat]
		if !ok {
			a = &sectionAgg{name: cat}
			agg[cat] = a
		}
		a.compressedBytes += s.CompressedSize
		a.uncompressedBytes += s.UncompressedSize
		a.count++
	}

	sorted := make([]*sectionAgg, 0, len(agg))
	for _, a := range agg {
		sorted = append(sorted, a)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].compressedBytes > sorted[j].compressedBytes
	})

	fmt.Printf("%-40s %12s %12s %8s %8s\n", "Section", "Compressed", "Uncompressed", "Count", "% File")
	fmt.Printf("%-40s %12s %12s %8s %8s\n", "-------", "----------", "------------", "-----", "------")
	for _, a := range sorted {
		pct := 100.0 * float64(a.compressedBytes) / float64(report.FileSize)
		compStr := humanBytes(a.compressedBytes)
		uncompStr := ""
		if a.uncompressedBytes > 0 {
			uncompStr = humanBytes(a.uncompressedBytes)
		}
		fmt.Printf("%-40s %12s %12s %8d %7.1f%%\n", a.name, compStr, uncompStr, a.count, pct)
	}

	// Per-column breakdown.
	fmt.Println()
	fmt.Println("=== Per-column breakdown (all blocks aggregated) ===")

	type colAgg struct {
		name              string
		encoding          string
		compressedBytes   int64
		uncompressedBytes int64
		count             int
	}
	colMap := map[string]*colAgg{}
	for _, s := range report.Sections {
		if s.ColumnName == "" {
			continue
		}
		c, ok := colMap[s.ColumnName]
		if !ok {
			c = &colAgg{name: s.ColumnName, encoding: s.Encoding}
			colMap[s.ColumnName] = c
		}
		c.compressedBytes += s.CompressedSize
		c.uncompressedBytes += s.UncompressedSize
		c.count++
	}

	colSorted := make([]*colAgg, 0, len(colMap))
	for _, c := range colMap {
		colSorted = append(colSorted, c)
	}
	sort.Slice(colSorted, func(i, j int) bool {
		return colSorted[i].compressedBytes > colSorted[j].compressedBytes
	})

	fmt.Printf("%-50s %-15s %12s %12s %8s\n", "Column", "Encoding", "Compressed", "Uncompressed", "% File")
	fmt.Printf("%-50s %-15s %12s %12s %8s\n", "------", "--------", "----------", "------------", "------")
	for _, c := range colSorted {
		pct := 100.0 * float64(c.compressedBytes) / float64(report.FileSize)
		compStr := humanBytes(c.compressedBytes)
		uncompStr := ""
		if c.uncompressedBytes > 0 {
			uncompStr = humanBytes(c.uncompressedBytes)
		}
		fmt.Printf("%-50s %-15s %12s %12s %7.1f%%\n", c.name, c.encoding, compStr, uncompStr, pct)
	}

	// Intrinsic column breakdown.
	hasIntrinsic := false
	for _, s := range report.Sections {
		if len(s.Section) > 10 && s.Section[:10] == "intrinsic." && s.ColumnName != "" {
			hasIntrinsic = true
			break
		}
	}
	if hasIntrinsic {
		fmt.Println()
		fmt.Println("=== Intrinsic column breakdown ===")
		type icAgg struct {
			name     string
			encoding string
			colType  string
			bytes    int64
		}
		var icCols []*icAgg
		var tocBytes int64
		for _, s := range report.Sections {
			if s.Section == "intrinsic.toc" {
				tocBytes = s.CompressedSize
				continue
			}
			if len(s.Section) > 10 && s.Section[:10] == "intrinsic." && s.ColumnName != "" {
				icCols = append(icCols, &icAgg{
					name:     s.ColumnName,
					encoding: s.Encoding,
					colType:  s.ColumnType,
					bytes:    s.CompressedSize,
				})
			}
		}
		sort.Slice(icCols, func(i, j int) bool {
			return icCols[i].bytes > icCols[j].bytes
		})
		fmt.Printf("%-40s %-10s %-10s %12s %8s\n", "Column", "Type", "Format", "Size", "% File")
		fmt.Printf("%-40s %-10s %-10s %12s %8s\n", "------", "----", "------", "----", "------")
		for _, c := range icCols {
			pct := 100.0 * float64(c.bytes) / float64(report.FileSize)
			fmt.Printf("%-40s %-10s %-10s %12s %7.1f%%\n", c.name, c.colType, c.encoding, humanBytes(c.bytes), pct)
		}
		if tocBytes > 0 {
			pct := 100.0 * float64(tocBytes) / float64(report.FileSize)
			fmt.Printf("%-40s %-10s %-10s %12s %7.1f%%\n", "(TOC)", "", "", humanBytes(tocBytes), pct)
		}
	}

	if len(os.Args) > 2 && os.Args[2] == "-json" {
		fmt.Println()
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(report)
	}
}

func categorize(section string) string {
	if len(section) > 5 && section[:5] == "block" {
		if strContains(section, ".column[") && strContains(section, "].data") {
			return "block.column_data"
		}
		if strContains(section, ".column_metadata") {
			return "block.column_metadata"
		}
		if strContains(section, ".header") {
			return "block.header"
		}
		return "block.other"
	}
	if strContains(section, "metadata.range_index.column") {
		return "metadata.range_index"
	}
	return section
}

func strContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func humanBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	}
	return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
}
