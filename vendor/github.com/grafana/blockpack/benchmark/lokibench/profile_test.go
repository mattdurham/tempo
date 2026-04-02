package lokibench

import (
	"fmt"
	"os"
	"testing"
)

// TestProfileBlockLayout prints detailed per-block statistics showing
// how streams are distributed across blocks with the stream-clustered sort.
func TestProfileBlockLayout(t *testing.T) {
	dir := chunksDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skipf("chunks dir %s not found", dir)
	}

	streams := decodeRealChunks(t, dir, defaultTenant, realDataMaxMB)
	if len(streams) == 0 {
		t.Skip("no streams")
	}

	// Write to blockpack
	outDir := t.TempDir()
	bpStore, err := NewBlockpackStore(outDir)
	if err != nil {
		t.Fatal(err)
	}
	const batchSize = 100
	for i := 0; i < len(streams); i += batchSize {
		end := min(i+batchSize, len(streams))
		if err := bpStore.Write(t.Context(), streams[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	if err := bpStore.Close(); err != nil {
		t.Fatal(err)
	}

	bpReader, err := openBlockpackReader(bpStore.Path())
	if err != nil {
		t.Fatal(err)
	}

	totalBlocks := bpReader.BlockCount()
	t.Logf("Total blocks: %d", totalBlocks)

	var totalRows, clusterMatchRows int
	var blocksWithClusterMatch int
	// Track: per block, how many unique streams and how many rows per stream
	type blockInfo struct {
		rows          int
		clusterMatch  int
		uniqueStreams int
		streamsPerRun []int // consecutive run lengths of same-stream rows
	}
	var blockInfos []blockInfo

	for i := range totalBlocks {
		bwb, err := bpReader.GetBlockWithBytes(i, nil, nil)
		if err != nil {
			continue
		}
		blk := bwb.Block
		rowCount := blk.SpanCount()
		clusterCol := blk.GetColumn("resource.cluster")
		labelsCol := blk.GetColumn("resource.__loki_labels__")

		var matching int
		var prevLabels string
		var currentRunLen int
		var runs []int
		uniqueStreams := make(map[string]struct{})

		for row := range rowCount {
			if clusterCol != nil {
				val, _ := clusterCol.StringValue(row)
				if val == "dev-us-central-0" {
					matching++
				}
			}
			if labelsCol != nil {
				lbl, _ := labelsCol.StringValue(row)
				uniqueStreams[lbl] = struct{}{}
				if lbl == prevLabels {
					currentRunLen++
				} else {
					if currentRunLen > 0 {
						runs = append(runs, currentRunLen)
					}
					currentRunLen = 1
					prevLabels = lbl
				}
			}
		}
		if currentRunLen > 0 {
			runs = append(runs, currentRunLen)
		}

		totalRows += rowCount
		clusterMatchRows += matching
		if matching > 0 {
			blocksWithClusterMatch++
		}

		blockInfos = append(blockInfos, blockInfo{
			rows:          rowCount,
			clusterMatch:  matching,
			uniqueStreams: len(uniqueStreams),
			streamsPerRun: runs,
		})
	}

	// Print summary
	fmt.Printf("\n=== Block Layout Profile ===\n")
	fmt.Printf("Total blocks: %d\n", totalBlocks)
	fmt.Printf("Total rows: %d\n", totalRows)
	fmt.Printf("Rows matching cluster=dev-us-central-0: %d (%.1f%%)\n",
		clusterMatchRows, float64(clusterMatchRows)/float64(totalRows)*100)
	fmt.Printf("Blocks with cluster match: %d / %d (%.0f%%)\n",
		blocksWithClusterMatch, totalBlocks,
		float64(blocksWithClusterMatch)/float64(totalBlocks)*100)
	fmt.Printf("Rows scanned but not matching: %d\n\n", totalRows-clusterMatchRows)

	// Print per-block details for first 20 and any with matches
	printed := 0
	for i, bi := range blockInfos {
		if printed >= 30 && bi.clusterMatch == 0 {
			continue
		}
		// Compute avg and max run length
		var maxRun, sumRun int
		for _, r := range bi.streamsPerRun {
			sumRun += r
			if r > maxRun {
				maxRun = r
			}
		}
		avgRun := 0
		if len(bi.streamsPerRun) > 0 {
			avgRun = sumRun / len(bi.streamsPerRun)
		}

		marker := " "
		if bi.clusterMatch > 0 {
			marker = "*"
		}
		fmt.Printf("%s block %3d: %5d rows, %3d streams, %4d cluster-match, runs=%d avg=%d max=%d\n",
			marker, i, bi.rows, bi.uniqueStreams, bi.clusterMatch,
			len(bi.streamsPerRun), avgRun, maxRun)
		printed++
	}

	fmt.Printf("\n=== Implication ===\n")
	fmt.Printf("With stream-run detection, for {cluster=\"dev-us-central-0\"}:\n")
	fmt.Printf("  Current: scan %d rows across %d blocks\n", totalRows, blocksWithClusterMatch)
	fmt.Printf("  With run-skip: check matchers %d times (once per stream run), skip %d non-matching rows\n",
		totalBlocks*5, totalRows-clusterMatchRows) // rough estimate
}
