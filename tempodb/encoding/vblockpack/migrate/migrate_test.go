package migrate

import "testing"

func TestSplitStatements_HandlesLineCommentsWithSemicolons(t *testing.T) {
	sql := `
-- a comment; with a semicolon in it, not a statement boundary
CREATE TABLE IF NOT EXISTS foo (id INT); -- another comment; also with one
CREATE INDEX IF NOT EXISTS idx_foo ON foo (id);
`
	got := SplitStatements(sql)
	want := []string{
		"CREATE TABLE IF NOT EXISTS foo (id INT)",
		"CREATE INDEX IF NOT EXISTS idx_foo ON foo (id)",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d statements, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statement %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSplitStatements_SkipsBlankStatements(t *testing.T) {
	got := SplitStatements("  ;;\n-- only a comment\n  ;")
	if len(got) != 0 {
		t.Fatalf("expected 0 statements from all-blank/comment input, got %v", got)
	}
}
