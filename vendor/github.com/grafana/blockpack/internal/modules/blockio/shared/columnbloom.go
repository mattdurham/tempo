package shared

// columnbloom.go — per-block bloom filter over column names present in a block (issue
// #531, NOTE-COLUMNBLOOM-1).
//
// This is a NEW mechanism, not a revival of the OLD (2026-03-07-removed) ColumnNameBloom:
// that field was inlined into the fixed-layout block index entries, so removing it was a
// breaking wire-format change (NOTE-BLOOM-REMOVAL, blockio/NOTES.md). This filter instead
// lives in its OWN optional ToC section (ToCSubTypeColumnBloom) alongside — but separate
// from — the block index, so its absence in a file (written before this change, or by any
// writer that never computed it) degrades gracefully: BlockMeta.ColumnBloom is simply
// nil/empty, and callers MUST treat that as "no information, don't prune, must fetch,"
// never as "definitely absent."
//
// NOTE-BLOOM-REMOVAL's rationale ("CMS subsumes column-name bloom") is now stale: CMS
// (Count-Min Sketch) was never actually implemented as a real per-block presence check in
// this codebase (grep confirms no BlockCMS/CMS type anywhere in the module tree — only
// aspirational prose in NOTES.md and the docs-site wiki), and the sketch/pruning subsystem
// it would have lived in was itself fully removed later (#435 KLL sketch, #437 file-level
// bloom, #439 range index). There is currently no other column-presence signal anywhere in
// the file format; this is a genuinely new, from-scratch mechanism, not a reinstatement.
//
// The hash construction mirrors internal/modules/valueindex/bucketbloom.go's
// valueBloomHashes exactly (double FNV-1a, Kirsch-Mitzenmacher probing) for consistency
// with this codebase's other bloom filter, just sized for a FIXED per-block byte budget
// instead of scaling with distinct-value count.

// ColumnBloomBytes is the fixed per-block bloom filter size: 256 bits. Matches the byte
// size of the old (2026-03-07-removed) ColumnNameBloom field exactly — a reasonable,
// already-precedented size for a per-block column-name set (typically tens of columns).
const ColumnBloomBytes = 32

// columnBloomK is the number of hash probes per column name (Kirsch-Mitzenmacher double
// hashing), matching valueindex's bucketbloom.go tuning (k=7) for the sibling value bloom.
const columnBloomK = 7

// columnNameBloomHashes returns two independent 64-bit hashes for name: h1 is FNV-1a: h2
// is a salted FNV-1a forced odd so the double-hashing stride is co-prime with the bit
// count for any power-of-two-ish modulus, giving good probe distribution. Mirrors
// valueindex.valueBloomHashes's construction exactly, operating on a string instead of a
// []byte.
func columnNameBloomHashes(name string) (uint64, uint64) {
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)
	h1 := uint64(fnvOffset)
	for i := range len(name) {
		h1 ^= uint64(name[i])
		h1 *= fnvPrime
	}
	h2 := uint64(fnvOffset) ^ 0x9E3779B97F4A7C15
	for i := range len(name) {
		h2 ^= uint64(name[i])
		h2 *= fnvPrime
	}
	return h1, h2 | 1
}

// AddColumnNameToBloom sets the bits for name in bloom. No-op for an empty/nil filter, or
// a filter whose length isn't ColumnBloomBytes (defensive — the writer always allocates
// exactly ColumnBloomBytes via BuildColumnBloom).
func AddColumnNameToBloom(bloom []byte, name string) {
	if len(bloom) == 0 {
		return
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := columnNameBloomHashes(name)
	for i := range uint64(columnBloomK) {
		pos := (h1 + i*h2) % m
		bloom[pos/8] |= 1 << (pos % 8) //nolint:gosec // pos%8 ∈ 0..7
	}
}

// TestColumnNameBloom returns false only if name is definitely absent from bloom (no
// false negatives, by construction of a bloom filter). Returns true (conservative — "no
// information, don't prune") for an empty/nil filter, which is exactly what an
// old/absent-bloom block's BlockMeta.ColumnBloom looks like.
func TestColumnNameBloom(bloom []byte, name string) bool {
	if len(bloom) == 0 {
		return true
	}
	m := uint64(len(bloom)) * 8
	h1, h2 := columnNameBloomHashes(name)
	for i := range uint64(columnBloomK) {
		pos := (h1 + i*h2) % m
		if bloom[pos/8]&(1<<(pos%8)) == 0 { //nolint:gosec // pos%8 ∈ 0..7
			return false
		}
	}
	return true
}

// BuildColumnBloom constructs a fresh ColumnBloomBytes-sized bloom filter containing every
// name in columnNames. Used by the writer at block-flush time (writer.mergeBuiltBlock) to
// compute a block's bloom from its OWN actual, just-built column set — this runs
// identically for a fresh-ingestion block and a compaction-merged output block, so a
// compacted block's bloom always reflects its real merged column set, regardless of
// whether any of the input blocks being merged carried a bloom at all (issue #531 point 3).
func BuildColumnBloom(columnNames []string) []byte {
	bloom := make([]byte, ColumnBloomBytes)
	for _, name := range columnNames {
		AddColumnNameToBloom(bloom, name)
	}
	return bloom
}
