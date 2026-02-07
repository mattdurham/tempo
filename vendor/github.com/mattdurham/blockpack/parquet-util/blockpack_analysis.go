package parquetutil

import (
	"fmt"
	"os"
	"strings"

	ondiskio "github.com/mattdurham/blockpack/blockpack/io"
)

func analyzeBlockpackToMarkdown(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	reader, err := ondiskio.NewReader(data)
	if err != nil {
		return "", err
	}

	blocks := reader.Blocks()

	var sb strings.Builder

	// File-level summary
	sb.WriteString("## Blockpack File Analysis\n\n")
	sb.WriteString(fmt.Sprintf("- Blocks: %d\n", len(blocks)))
	sb.WriteString(fmt.Sprintf("- Total Spans: %d\n\n", reader.SpanCount()))

	// Block-level details
	sb.WriteString("### Block Details\n\n")
	sb.WriteString("| Block | Span Count | Columns |\n")
	sb.WriteString("|-------|------------|---------|" + "\n")

	for idx := range blocks {
		// Load the actual block to get column count
		block, err := reader.GetBlock(idx)
		if err != nil {
			continue
		}
		sb.WriteString(fmt.Sprintf("| %d | %d | %d |\n",
			idx,
			block.SpanCount(),
			len(block.Columns()),
		))
	}
	sb.WriteString("\n")

	return sb.String(), nil
}
