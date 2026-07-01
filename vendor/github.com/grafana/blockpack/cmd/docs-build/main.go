// Package main is the docs site builder for Blockpack.
// It reads wiki Markdown files from ../blockpack-worktrees/wiki/ and writes
// static HTML pages to docs/.
//
// Run from the blockpack repo root:
//
//	go run ./cmd/docs-build
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// page describes one output HTML file.

// relative to docs/
// wiki source filename (README.md, KLL.md, ...)

// mp4 filename in docs/videos/, empty if none
// URL path segment used for active-link detection

var pages = []page{
	{output: "index.html", wiki: "README.md", title: "Home", slug: "/"},
	{output: "file-format.html", wiki: "File-Format.md", title: "File Format", slug: "/file-format"},
	{
		output: "execution-path.html",
		wiki:   "Execution-Path.md",
		title:  "Execution Path",
		video:  "execution-path.mp4",
		slug:   "/execution-path",
	},
	{
		output: "write-path.html",
		wiki:   "Write-Path.md",
		title:  "Write Path",
		video:  "write-path.mp4",
		slug:   "/write-path",
	},
	{output: "kll.html", wiki: "KLL.md", title: "KLL Sketches", video: "kll.mp4", slug: "/kll"},
	{output: "cms.html", wiki: "CMS.md", title: "Count-Min Sketch", video: "cms.mp4", slug: "/cms"},
	{output: "topk.html", wiki: "TopK.md", title: "TopK", video: "topk.mp4", slug: "/topk"},
	{output: "hll.html", wiki: "HLL.md", title: "HyperLogLog", video: "hll.mp4", slug: "/hll"},
	{
		output: "bloom-filter.html",
		wiki:   "Bloom-Filter.md",
		title:  "Bloom Filters",
		video:  "bloom.mp4",
		slug:   "/bloom-filter",
	},
	{
		output: "intrinsic-columns.html",
		wiki:   "Intrinsic-Columns.md",
		title:  "Intrinsic Columns",
		video:  "intrinsic.mp4",
		slug:   "/intrinsic-columns",
	},
	{
		output: "block-scoring.html",
		wiki:   "Block-Scoring.md",
		title:  "Block Scoring",
		video:  "block-scoring.mp4",
		slug:   "/block-scoring",
	},
	{
		output: "block-format.html",
		wiki:   "Block-Format.md",
		title:  "Block Format",
		video:  "block-format.mp4",
		slug:   "/block-format",
	},
	{
		output: "production-results.html",
		wiki:   "Production-Results.md",
		title:  "Production Results",
		slug:   "/production-results",
	},
}

// nav items for the sidebar, in order.

// section label (rendered as <div class="section-label">), empty if link
// URL href
// link text

var navItems = []navItem{
	{isLabel: true, label: "Overview"},
	{href: "/", text: "Home"},
	{href: "/file-format", text: "File Format"},
	{isLabel: true, label: "Query Execution"},
	{href: "/execution-path", text: "Execution Path"},
	{href: "/write-path", text: "Write Path"},
	{isLabel: true, label: "Indexes"},
	{href: "/kll", text: "KLL Sketches"},
	{href: "/cms", text: "Count-Min Sketch"},
	{href: "/topk", text: "TopK"},
	{href: "/hll", text: "HyperLogLog"},
	{href: "/bloom-filter", text: "Bloom Filters"},
	{href: "/intrinsic-columns", text: "Intrinsic Columns"},
	{isLabel: true, label: "Blocks"},
	{href: "/block-scoring", text: "Block Scoring"},
	{href: "/block-format", text: "Block Format"},
	{isLabel: true, label: "Performance"},
	{href: "/benchmarks/", text: "Benchmarks"},
	{href: "/production-results", text: "Production Results"},
}

func buildNav(activeSlug string) string {
	var sb strings.Builder
	sb.WriteString("<nav class=\"sidebar\">\n")
	sb.WriteString("  <div class=\"sidebar-brand\">BLOCKPACK</div>\n")
	for _, item := range navItems {
		if item.isLabel {
			fmt.Fprintf(&sb, "  <div class=\"section-label\">%s</div>\n", item.label)
			continue
		}
		if item.href == activeSlug {
			fmt.Fprintf(&sb, "  <a href=\"%s\" class=\"active\">%s</a>\n", item.href, item.text)
		} else {
			fmt.Fprintf(&sb, "  <a href=\"%s\">%s</a>\n", item.href, item.text)
		}
	}
	sb.WriteString("</nav>")
	return sb.String()
}

func buildPage(p page, wikiDir string) (string, error) {
	srcPath := filepath.Join(wikiDir, p.wiki)
	data, err := os.ReadFile(srcPath) //nolint:gosec // srcPath is constructed from known pages, not user input
	if err != nil {
		return "", fmt.Errorf("read %s: %w", srcPath, err)
	}

	content := convertMarkdown(string(data))

	// Insert video block after first <h1> if a video is configured.
	if p.video != "" {
		videoHTML := fmt.Sprintf(
			"<div class=\"video-wrap\">\n  <video autoplay loop muted playsinline controls>\n    <source src=\"/videos/%s\" type=\"video/mp4\">\n  </video>\n</div>\n",
			p.video,
		)
		// Find first </h1> and insert video immediately after.
		if idx := strings.Index(content, "</h1>"); idx != -1 {
			content = content[:idx+5] + "\n" + videoHTML + content[idx+5:]
		}
	}

	nav := buildNav(p.slug)

	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>%s — Blockpack</title>
  <link rel="stylesheet" href="/assets/css/style.css">
</head>
<body>
  <header class="topbar">
    <a href="/" class="topbar-brand">Blockpack Docs</a>
    <a href="https://github.com/grafana/blockpack" class="topbar-github">GitHub →</a>
  </header>
  <div class="layout">
    %s
    <main class="content">
%s
    </main>
  </div>
</body>
</html>
`, p.title, nav, content)

	return html, nil
}

// regexps used by convertMarkdown.
var (
	reBold         = regexp.MustCompile(`\*\*(.+?)\*\*`)
	reItalicUS     = regexp.MustCompile(`_(.+?)_`)
	reItalicStar   = regexp.MustCompile(`\*(.+?)\*`)
	reCode         = regexp.MustCompile("`([^`]+)`")
	reLink         = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)
	reSeparatorRow = regexp.MustCompile(`^\|[\s\-|:]+\|$`)
)

// mdState holds the mutable state for convertMarkdown's line-by-line parser.

func (s *mdState) flushParagraph() {
	if s.inParagraph {
		s.sb.WriteString("</p>\n")
		s.inParagraph = false
	}
}

func (s *mdState) closeTable() {
	if s.inTable {
		s.sb.WriteString("</tbody>\n</table>\n")
		s.inTable = false
	}
}

// processSkipSection handles ## Animation section tracking. Returns true if the
// line was consumed (skipped or started the skip zone).
func (s *mdState) processSkipSection(line string) bool {
	if strings.HasPrefix(line, "## Animation") {
		s.skipSection = true
		s.flushParagraph()
		return true
	}
	if s.skipSection {
		if strings.HasPrefix(line, "## ") || strings.HasPrefix(line, "# ") {
			s.skipSection = false
			return false // fall through to process heading normally
		}
		return true // still in skip zone
	}
	return false
}

// processCodeBlock handles fenced code block open/close. Returns (consumed, newIndex).
func (s *mdState) processCodeBlock(line string) bool {
	if !s.inCode && strings.HasPrefix(line, "```") {
		s.flushParagraph()
		s.closeTable()
		lang := strings.TrimSpace(strings.TrimPrefix(line, "```"))
		if lang != "" {
			fmt.Fprintf(&s.sb, "<pre><code class=\"language-%s\">", htmlEscape(lang))
		} else {
			s.sb.WriteString("<pre><code>")
		}
		s.inCode = true
		return true
	}
	if s.inCode {
		if strings.HasPrefix(line, "```") {
			s.sb.WriteString("</code></pre>\n")
			s.inCode = false
		} else {
			s.sb.WriteString(htmlEscape(line))
			s.sb.WriteByte('\n')
		}
		return true
	}
	return false
}

// processHeading handles ATX headings (####, ###, ##, #). Returns true if consumed.
func (s *mdState) processHeading(line string) bool {
	for _, h := range []struct {
		prefix string
		tag    string
	}{
		{"#### ", "h4"}, {"### ", "h3"}, {"## ", "h2"}, {"# ", "h1"},
	} {
		if strings.HasPrefix(line, h.prefix) {
			s.flushParagraph()
			text := inlineMarkdown(strings.TrimPrefix(line, h.prefix))
			fmt.Fprintf(&s.sb, "<%s>%s</%s>\n", h.tag, text, h.tag)
			return true
		}
	}
	return false
}

// processTableRow handles a table row line. Returns true if consumed.
func (s *mdState) processTableRow(line string) bool {
	if !strings.HasPrefix(line, "|") {
		return false
	}
	if reSeparatorRow.MatchString(strings.TrimSpace(line)) {
		return true // skip separator row
	}
	s.flushParagraph()
	cells := parseTableRow(line)
	if !s.inTable {
		s.sb.WriteString("<table>\n<thead>\n<tr>")
		for _, cell := range cells {
			fmt.Fprintf(&s.sb, "<th>%s</th>", inlineMarkdown(cell))
		}
		s.sb.WriteString("</tr>\n</thead>\n<tbody>\n")
		s.inTable = true
		return true
	}
	s.sb.WriteString("<tr>")
	for _, cell := range cells {
		fmt.Fprintf(&s.sb, "<td>%s</td>", inlineMarkdown(cell))
	}
	s.sb.WriteString("</tr>\n")
	return true
}

// processUnorderedList collects consecutive unordered list items starting at lines[i].
// Returns the new index after all consumed items.
func (s *mdState) processUnorderedList(lines []string, i int) int {
	s.flushParagraph()
	s.sb.WriteString("<ul>\n")
	for i < len(lines) && (strings.HasPrefix(lines[i], "- ") || strings.HasPrefix(lines[i], "* ")) {
		item := strings.TrimPrefix(lines[i], "- ")
		item = strings.TrimPrefix(item, "* ")
		fmt.Fprintf(&s.sb, "<li>%s</li>\n", inlineMarkdown(item))
		i++
	}
	s.sb.WriteString("</ul>\n")
	return i - 1 // caller will i++ so we back up one
}

// processOrderedList collects consecutive ordered list items starting at lines[i].
// Returns the new index after all consumed items.
func (s *mdState) processOrderedList(lines []string, i int) int {
	s.flushParagraph()
	s.sb.WriteString("<ol>\n")
	for i < len(lines) && isOrderedListItem(lines[i]) {
		item := orderedListContent(lines[i])
		fmt.Fprintf(&s.sb, "<li>%s</li>\n", inlineMarkdown(item))
		i++
	}
	s.sb.WriteString("</ol>\n")
	return i - 1 // caller will i++ so we back up one
}

// convertMarkdown performs a simple line-by-line conversion of Markdown to HTML.
func convertMarkdown(md string) string {
	lines := strings.Split(md, "\n")
	var s mdState

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		if s.processSkipSection(line) {
			continue
		}
		if s.processCodeBlock(line) {
			continue
		}
		if strings.HasPrefix(line, "![") {
			continue // skip image lines
		}
		if s.inTable && !strings.HasPrefix(line, "|") {
			s.closeTable()
		}
		if s.processHeading(line) {
			continue
		}
		if strings.TrimSpace(line) == "---" {
			s.flushParagraph()
			s.sb.WriteString("<hr>\n")
			continue
		}
		if s.processTableRow(line) {
			continue
		}
		if strings.TrimSpace(line) == "" {
			s.flushParagraph()
			continue
		}
		if strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") {
			i = s.processUnorderedList(lines, i)
			continue
		}
		if isOrderedListItem(line) {
			i = s.processOrderedList(lines, i)
			continue
		}
		// Regular paragraph text.
		if !s.inParagraph {
			s.sb.WriteString("<p>")
			s.inParagraph = true
		} else {
			s.sb.WriteByte('\n')
		}
		s.sb.WriteString(inlineMarkdown(line))
	}

	s.flushParagraph()
	s.closeTable()
	if s.inCode {
		s.sb.WriteString("</code></pre>\n")
	}

	return s.sb.String()
}

// inlineMarkdown converts inline Markdown elements within a single line of text.
func inlineMarkdown(s string) string {
	// Links first so we don't double-process their content.
	s = reLink.ReplaceAllStringFunc(s, func(match string) string {
		sub := reLink.FindStringSubmatch(match)
		if len(sub) != 3 {
			return match
		}
		text, href := sub[1], sub[2]
		href = fixInternalLink(href)
		return fmt.Sprintf("<a href=\"%s\">%s</a>", href, text)
	})

	// Replace code spans with numbered placeholders BEFORE bold/italic
	// processing so that underscores inside backticks are never treated as
	// italic markers (e.g. `header_offset` must not become header<em>offset</em>).
	var codePlaceholders []string
	s = reCode.ReplaceAllStringFunc(s, func(match string) string {
		inner := reCode.FindStringSubmatch(match)
		if len(inner) != 2 {
			return match
		}
		ph := fmt.Sprintf("\x00CODE%d\x00", len(codePlaceholders))
		codePlaceholders = append(codePlaceholders, "<code>"+htmlEscape(inner[1])+"</code>")
		return ph
	})

	s = reBold.ReplaceAllString(s, "<strong>$1</strong>")
	// Italic: avoid matching * inside <strong> tags; use underscore and star variants.
	s = reItalicUS.ReplaceAllString(s, "<em>$1</em>")
	// Only apply star italic if not preceded by another star (already handled by bold).
	s = reItalicStar.ReplaceAllString(s, "<em>$1</em>")

	// Restore code span placeholders.
	for i, html := range codePlaceholders {
		s = strings.ReplaceAll(s, fmt.Sprintf("\x00CODE%d\x00", i), html)
	}
	return s
}

// fixInternalLink converts wiki-style .md links to lowercase slugs.
// e.g. "KLL.md" -> "/kll", "Bloom-Filter.md" -> "/bloom-filter"
// External links (http/https) are returned unchanged.
func fixInternalLink(href string) string {
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href
	}
	if strings.HasSuffix(href, ".md") {
		slug := strings.TrimSuffix(href, ".md")
		slug = strings.ToLower(slug)
		return "/" + slug
	}
	return href
}

// parseTableRow splits a Markdown table row into cell contents.
func parseTableRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	parts := strings.Split(line, "|")
	cells := make([]string, 0, len(parts))
	for _, p := range parts {
		cells = append(cells, strings.TrimSpace(p))
	}
	return cells
}

// isOrderedListItem returns true if the line looks like "1. text".
func isOrderedListItem(line string) bool {
	for i, ch := range line {
		if ch >= '0' && ch <= '9' {
			continue
		}
		if ch == '.' && i > 0 && len(line) > i+1 && line[i+1] == ' ' {
			return true
		}
		break
	}
	return false
}

// orderedListContent strips the "1. " prefix.
func orderedListContent(line string) string {
	for i, ch := range line {
		if ch == '.' {
			return strings.TrimSpace(line[i+1:])
		}
	}
	return line
}

// htmlEscape escapes <, >, &, " for safe HTML embedding.
func htmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

func main() {
	// Resolve repo root: the directory this command is run from.
	repoRoot, err := os.Getwd()
	if err != nil {
		log.Fatalf("getwd: %v", err)
	}

	// Wiki source directory is the sibling worktree.
	wikiDir := filepath.Join(repoRoot, "..", "blockpack-worktrees", "wiki")
	docsDir := filepath.Join(repoRoot, "docs")

	if err := os.MkdirAll(docsDir, 0o755); err != nil { //nolint:gosec // 0755 is standard for public docs directories
		log.Fatalf("mkdir docs: %v", err)
	}

	for _, p := range pages {
		html, err := buildPage(p, wikiDir)
		if err != nil {
			log.Fatalf("build %s: %v", p.output, err)
		}
		outPath := filepath.Join(docsDir, p.output)
		if err := os.WriteFile(outPath, []byte(html), 0o644); err != nil { //nolint:gosec // 0644 is standard for public HTML files
			log.Fatalf("write %s: %v", outPath, err)
		}
		fmt.Printf("docs/%s\n", p.output)
	}
}
