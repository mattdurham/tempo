package blockpack

// column_manifest.go — public glue for internal/modules/colhashmanifest's
// native Postgres Store implementation (issue #506). First-ever root
// re-export for colhashmanifest: this module previously had no public root
// surface at all (its Store/Entry/Load/RecordColumn types are wired directly
// by tempo's valueindexconsumer/valuecountscompactor call sites via
// structurally-typed interfaces, no nominal type re-export needed for the
// blob path). This IS genuinely new root public API surface — flagged
// explicitly per CLAUDE.md's "no new public API surface without explicit
// user permission": issue #506's own text is the permission granted here
// (tempo needs a way to construct a Postgres-backed store to assign to
// Config.ManifestStore, a structurally-typed interface any *PgStore
// satisfies without further re-export).

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/blockpack/internal/modules/colhashmanifest"
)

var _ colhashmanifest.Store = (*colhashmanifest.PgStore)(nil)

// NewPgColumnManifestStore constructs a Postgres-backed colhashmanifest.Store
// over pool. Returns the root-owned interface type (colhashmanifest.Store) rather
// than the internal *colhashmanifest.PgStore concrete type, matching
// NewPgCubeEntryStore's/NewPgViUsageEntryStore's interface-return shape (review.md
// Issue 3). tempo assigns the result to valueindexconsumer.Config.ManifestStore /
// valuecountscompactor.Config.ManifestStore — both are locally-defined structural
// interfaces requiring exactly Get(ctx, key) ([]byte, error) and Put(ctx, key, data)
// error, which colhashmanifest.Store's own method set matches exactly, so the
// returned value is directly assignable there with no wrapper needed.
func NewPgColumnManifestStore(pool *pgxpool.Pool) colhashmanifest.Store {
	return colhashmanifest.NewPgStore(pool)
}

// ApplyColumnManifestSchema applies colhashmanifest's Postgres schema against
// pool. Exported, never called automatically by any constructor — the
// embedding application calls it once at its own startup.
func ApplyColumnManifestSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return colhashmanifest.ApplySchema(ctx, pool)
}
