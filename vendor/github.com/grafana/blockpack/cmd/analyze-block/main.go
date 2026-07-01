// Command analyze-block uses the blockpack library to analyze a local .blockpack file.
// Prints a per-column compression and encoding breakdown across all inner blocks,
// and file-level TOC section sizes.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	blockpack "github.com/grafana/blockpack"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

func main() {
	path := flag.String("file", "", "path to .blockpack file")
	groups := flag.Bool("groups", false, "analyze (service.name, span.name) group size distribution")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "usage: analyze-block -file <path> [-groups]")
		os.Exit(1)
	}
	if *groups {
		if err := runGroups(*path); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*path); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// localProvider wraps an os.File as a blockpack.ReaderProvider.
type localProvider struct {
	f    *os.File
	size int64
}

func newLocalProvider(path string) (*localProvider, error) {
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &localProvider{f: f, size: fi.Size()}, nil
}

func (p *localProvider) Size() (int64, error) { return p.size, nil }
func (p *localProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.f.ReadAt(buf, off)
}
func (p *localProvider) Close() error { return p.f.Close() }

func run(path string) error {
	prov, err := newLocalProvider(path)
	if err != nil {
		return err
	}
	defer func() { _ = prov.Close() }()

	r, err := blockpack.NewReaderFromProvider(prov)
	if err != nil {
		return fmt.Errorf("open blockpack: %w", err)
	}

	fi, _ := os.Stat(path)
	fileSize := fi.Size()

	var totalSpans int64
	for i := range r.BlockCount() {
		totalSpans += int64(r.BlockMeta(i).SpanCount)
	}

	fmt.Printf("=== Blockpack File Analysis ===\n")
	fmt.Printf("  File:         %s\n", path)
	fmt.Printf("  File size:    %.2f MB\n", float64(fileSize)/1024/1024)
	fmt.Printf("  Inner blocks: %d\n", r.BlockCount())
	fmt.Printf("  Total spans:  %s\n", commaf(totalSpans))
	fmt.Printf("  Traces:       %s\n", commaf(int64(r.TraceCount())))

	// ── File-level TOC ────────────────────────────────────────────────────
	subtypeNames := map[uint32]string{
		modules_shared.ToCSubTypeBloom:        "FileBloom",
		modules_shared.ToCSubTypeBlockIndex:   "BlockIndex",
		modules_shared.ToCSubTypeTrace:        "TraceIndex(legacy)",
		modules_shared.ToCSubTypeTS:           "TSIndex",
		modules_shared.ToCSubTypeTraceChunked: "TraceChunked",
	}
	typeNames := map[uint32]string{
		modules_shared.ToCTypeMetadata: "Metadata",
	}

	entries := r.ToCEntries()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Offset < entries[j].Offset
	})

	fmt.Printf("\n=== File-level TOC sections (%d entries) ===\n", len(entries))
	fmt.Printf("  %-22s %-25s %12s %12s\n", "Type", "SubType", "Offset", "MB")
	fmt.Println("  " + strings.Repeat("-", 78))

	var totalSecBytes int64
	for _, e := range entries {
		tn := typeNames[e.Type]
		if tn == "" {
			tn = fmt.Sprintf("type_%d", e.Type)
		}
		sn := subtypeNames[e.SubType]
		if sn == "" {
			sn = fmt.Sprintf("sub_%d", e.SubType)
		}
		fmt.Printf("  %-22s %-25s %12d %12.2f\n", tn, sn, e.Offset, float64(e.CompressedBytes)/1024/1024)
		totalSecBytes += e.CompressedBytes
	}
	fmt.Printf("  %-22s %-25s %12s %12.2f\n", "TOTAL", "", "", float64(totalSecBytes)/1024/1024)

	// ── Per-column analysis ───────────────────────────────────────────────
	type colAgg struct {
		name        string
		colType     modules_shared.ColumnType
		kind        uint8
		compBytes   int64
		uncompBytes int64
		appearances int
	}
	colMap := make(map[string]*colAgg)
	var totalComp, totalUncomp int64

	for bi := range r.BlockCount() {
		meta := r.BlockMeta(bi)
		raw := make([]byte, meta.Length)
		if _, rerr := prov.f.ReadAt(raw, int64(meta.Offset)); rerr != nil { //nolint:gosec
			fmt.Fprintf(os.Stderr, "  warn: block %d read: %v\n", bi, rerr)
			continue
		}
		metas, perr := modules_reader.ParseColMetas(raw, meta)
		if perr != nil {
			fmt.Fprintf(os.Stderr, "  warn: block %d parse: %v\n", bi, perr)
			continue
		}
		for _, cm := range metas {
			c, ok := colMap[cm.Name]
			if !ok {
				c = &colAgg{name: cm.Name, colType: cm.ColType, kind: cm.Kind}
				colMap[cm.Name] = c
			}
			c.compBytes += int64(cm.CompressedBytes)
			c.uncompBytes += int64(cm.UncompressedLen)
			c.appearances++
			totalComp += int64(cm.CompressedBytes)
			totalUncomp += int64(cm.UncompressedLen)
		}
	}

	cols := make([]*colAgg, 0, len(colMap))
	for _, c := range colMap {
		cols = append(cols, c)
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].compBytes > cols[j].compBytes })

	kindNames := buildKindNames()

	fmt.Printf("\n=== Per-column breakdown (%d unique columns, sorted by compressed size) ===\n", len(cols))
	fmt.Printf("  %-50s %-14s %-30s %10s %12s %8s %8s %6s\n",
		"Column", "ValueType", "Encoding", "Comp(MB)", "Uncomp(MB)", "Ratio", "B/span", "Blocks")
	fmt.Println("  " + strings.Repeat("-", 148))

	for _, c := range cols {
		kindName := kindNames[c.kind]
		if kindName == "" {
			kindName = fmt.Sprintf("kind_%d", c.kind)
		}
		ratio := float64(0)
		if c.compBytes > 0 {
			ratio = float64(c.uncompBytes) / float64(c.compBytes)
		}
		bps := float64(c.compBytes) / float64(totalSpans)
		fmt.Printf("  %-50s %-14s %-30s %10.2f %12.2f %8.2fx %8.2f %6d\n",
			c.name, colTypeName(c.colType), kindName,
			float64(c.compBytes)/1024/1024,
			float64(c.uncompBytes)/1024/1024,
			ratio, bps, c.appearances)
	}
	fmt.Println("  " + strings.Repeat("-", 148))
	totalRatio := float64(0)
	if totalComp > 0 {
		totalRatio = float64(totalUncomp) / float64(totalComp)
	}
	fmt.Printf("  %-50s %-14s %-30s %10.2f %12.2f %8.2fx %8.2f\n",
		"TOTAL", "", "",
		float64(totalComp)/1024/1024, float64(totalUncomp)/1024/1024,
		totalRatio, float64(totalComp)/float64(totalSpans))

	// ── Encoding distribution ─────────────────────────────────────────────
	encTotals := make(map[string]int64)
	for _, c := range cols {
		k := kindNames[c.kind]
		if k == "" {
			k = fmt.Sprintf("kind_%d", c.kind)
		}
		encTotals[k] += c.compBytes
	}
	type encRow struct {
		name  string
		bytes int64
	}
	encs := make([]encRow, 0, len(encTotals))
	for k, v := range encTotals {
		encs = append(encs, encRow{k, v})
	}
	sort.Slice(encs, func(i, j int) bool { return encs[i].bytes > encs[j].bytes })
	fmt.Printf("\n=== Encoding distribution ===\n")
	for _, e := range encs {
		fmt.Printf("  %-35s %8.2f MB  %5.1f%%\n",
			e.name, float64(e.bytes)/1024/1024, float64(e.bytes)/float64(totalComp)*100)
	}

	// ── Size breakdown ───────────────────────────────────────────────────
	fmt.Printf("\n=== Size breakdown ===\n")
	fmt.Printf("  Inner block columns:  %8.2f MB  (%.1f%% of file)\n",
		float64(totalComp)/1024/1024, float64(totalComp)/float64(fileSize)*100)
	fmt.Printf("  File-level sections:  %8.2f MB  (%.1f%% of file)\n",
		float64(totalSecBytes)/1024/1024, float64(totalSecBytes)/float64(fileSize)*100)

	idCols := []string{"trace:id", "span:id", "span:parent_id"}
	var idBytes int64
	for _, name := range idCols {
		if c, ok := colMap[name]; ok {
			idBytes += c.compBytes
			fmt.Printf("  %-22s       %8.2f MB  (%.1f%% of columns)\n",
				name, float64(c.compBytes)/1024/1024,
				float64(c.compBytes)/float64(totalComp)*100)
		}
	}
	fmt.Printf("  ID columns total:     %8.2f MB  (%.1f%% of columns, %.1f%% of file)\n",
		float64(idBytes)/1024/1024,
		float64(idBytes)/float64(totalComp)*100,
		float64(idBytes)/float64(fileSize)*100)

	return nil
}

func colTypeName(ct modules_shared.ColumnType) string {
	names := map[modules_shared.ColumnType]string{
		modules_shared.ColumnTypeString:        "String",
		modules_shared.ColumnTypeInt64:         "Int64",
		modules_shared.ColumnTypeUint64:        "Uint64",
		modules_shared.ColumnTypeFloat64:       "Float64",
		modules_shared.ColumnTypeBool:          "Bool",
		modules_shared.ColumnTypeBytes:         "Bytes",
		modules_shared.ColumnTypeRangeInt64:    "RangeInt64",
		modules_shared.ColumnTypeRangeUint64:   "RangeUint64",
		modules_shared.ColumnTypeRangeDuration: "RangeDuration",
		modules_shared.ColumnTypeRangeFloat64:  "RangeFloat64",
		modules_shared.ColumnTypeRangeBytes:    "RangeBytes",
		modules_shared.ColumnTypeRangeString:   "RangeString",
	}
	if n, ok := names[ct]; ok {
		return n
	}
	return fmt.Sprintf("type_%d", ct)
}

func buildKindNames() map[uint8]string {
	return map[uint8]string{
		1: "Dictionary", 2: "SparseDictionary",
		3: "InlineBytes", 4: "SparseInlineBytes",
		5: "DeltaUint64", 6: "RLEIndexes", 7: "SparseRLEIndexes",
		8: "XORBytes", 9: "SparseXORBytes",
		10: "PrefixBytes", 11: "SparsePrefixBytes",
		12: "DeltaDictionary", 13: "SparseDeltaDictionary",
		14: "VectorF32",
		15: "DictionaryAllPresent", 16: "InlineBytesAllPresent",
		17: "DeltaUint64AllPresent", 18: "RLEIndexesAllPresent",
		19: "XORBytesAllPresent", 20: "PrefixBytesAllPresent",
		21: "DeltaDictionaryAllPresent",
		22: "DeltaUint64BitPacked", 23: "DeltaUint64BitPackedAllPresent",
		24: "XORBytesUniform", 25: "SparseXORBytesUniform",
		26: "InlineBytesUniform", 27: "SparseInlineBytesUniform",
		28: "XORBytesUniformAllPresent",
		39: "DeltaUint64Paged",
		40: "GorillaFloat64", 41: "GorillaFloat64AllPresent",
	}
}

func commaf(n int64) string {
	s := fmt.Sprintf("%d", n)
	var out []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c)) //nolint:gosec // safe: c is an ASCII digit (0-9)
	}
	return string(out)
}

func runGroups(path string) error {
	prov, err := newLocalProvider(path)
	if err != nil {
		return err
	}
	defer func() { _ = prov.Close() }()

	r, err := blockpack.NewReaderFromProvider(prov)
	if err != nil {
		return fmt.Errorf("open blockpack: %w", err)
	}

	type groupKey struct{ svc, op string }

	// For each inner block, record how many spans each (svc, op) group contributes.
	groupFragments := map[groupKey][]int{} // all per-block fragment sizes
	groupsPerBlock := make([]int, r.BlockCount())

	for bi := range r.BlockCount() {
		raw, rerr := r.ReadBlockRaw(bi)
		if rerr != nil {
			return fmt.Errorf("read block %d: %w", bi, rerr)
		}
		block, perr := r.ParseBlockFromBytes(raw, modules_reader.WantAll(), r.BlockMeta(bi))
		if perr != nil {
			return fmt.Errorf("parse block %d: %w", bi, perr)
		}

		svcCol := block.Block.GetColumn("resource.service.name")
		nameCol := block.Block.GetColumn("span.name")
		n := block.Block.SpanCount()

		inBlock := map[groupKey]int{}
		for i := range n {
			var svc, op string
			if svcCol != nil {
				svc, _ = svcCol.StringValue(i)
			}
			if nameCol != nil {
				op, _ = nameCol.StringValue(i)
			}
			inBlock[groupKey{svc, op}]++
		}
		groupsPerBlock[bi] = len(inBlock)
		for k, cnt := range inBlock {
			groupFragments[k] = append(groupFragments[k], cnt)
		}
	}

	// All fragment sizes (spans of one group within one inner block)
	allFrag := []int{}
	for _, frags := range groupFragments {
		allFrag = append(allFrag, frags...)
	}
	sort.Ints(allFrag)

	pct := func(p float64) int {
		idx := int(float64(len(allFrag)-1) * p / 100)
		return allFrag[idx]
	}

	// Total spans per unique group
	totalPerGroup := []int{}
	for _, frags := range groupFragments {
		t := 0
		for _, f := range frags {
			t += f
		}
		totalPerGroup = append(totalPerGroup, t)
	}
	sort.Ints(totalPerGroup)
	pctG := func(p float64) int {
		idx := int(float64(len(totalPerGroup)-1) * p / 100)
		return totalPerGroup[idx]
	}

	sort.Ints(groupsPerBlock)
	pctB := func(p float64) int {
		idx := int(float64(len(groupsPerBlock)-1) * p / 100)
		return groupsPerBlock[idx]
	}

	fmt.Printf("=== (service.name, span.name) group analysis ===\n")
	fmt.Printf("  Inner blocks:             %d\n", r.BlockCount())
	fmt.Printf("  Unique (svc, op) groups:  %d\n", len(groupFragments))
	fmt.Printf("\n=== Distinct groups per inner block ===\n")
	fmt.Printf("  p50=%-6d  p90=%-6d  p99=%-6d  max=%d\n",
		pctB(50), pctB(90), pctB(99), groupsPerBlock[len(groupsPerBlock)-1])

	fmt.Printf("\n=== Fragment size: spans of one group within one inner block ===\n")
	fmt.Printf("  (current MaxBlockSpans=2000; a fragment = how many spans of a group fit in a block)\n")
	fmt.Printf("  p10=%-6d  p25=%-6d  p50=%-6d  p75=%-6d  p90=%-6d  p95=%-6d  p99=%-6d  max=%d\n",
		pct(10), pct(25), pct(50), pct(75), pct(90), pct(95), pct(99), allFrag[len(allFrag)-1])

	// Histogram
	bounds := [][2]int{
		{1, 1},
		{2, 10},
		{11, 50},
		{51, 100},
		{101, 200},
		{201, 500},
		{501, 1000},
		{1001, 2000},
		{2001, 1 << 30},
	}
	labels := []string{"=1", "2-10", "11-50", "51-100", "101-200", "201-500", "501-1k", "1001-2k", ">2000"}
	fmt.Printf("\n=== Fragment size histogram ===\n")
	total := len(allFrag)
	for i, b := range bounds {
		cnt := 0
		for _, f := range allFrag {
			if f >= b[0] && f <= b[1] {
				cnt++
			}
		}
		bar := ""
		pv := float64(cnt) * 100 / float64(total)
		for j := 0; j < int(pv/2); j++ {
			bar += "█"
		}
		fmt.Printf("  %-10s  %7d  (%5.1f%%)  %s\n", labels[i], cnt, pv, bar)
	}

	fmt.Printf("\n=== Total spans per unique group (whole file) ===\n")
	fmt.Printf("  p50=%-6d  p75=%-6d  p90=%-6d  p95=%-6d  p99=%-6d  max=%d\n",
		pctG(50), pctG(75), pctG(90), pctG(95), pctG(99), totalPerGroup[len(totalPerGroup)-1])

	// How many groups have > 2000 total spans (i.e. would benefit from larger blocks)
	big := 0
	for _, t := range totalPerGroup {
		if t > 2000 {
			big++
		}
	}
	fmt.Printf("\n  Groups with >2000 total spans: %d / %d (%.1f%%)\n",
		big, len(totalPerGroup), float64(big)*100/float64(len(totalPerGroup)))

	return nil
}
