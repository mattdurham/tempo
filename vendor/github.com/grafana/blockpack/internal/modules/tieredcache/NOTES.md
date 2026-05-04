# NOTES: tieredcache

## NOTE-TC-001: New Package vs. Extending chaincache

`chaincache` implements depth-wise tiering: fastest-first ordering for the same key space.
`tieredcache` implements width-wise routing: key-space partitioning across two independent
sub-caches. These are orthogonal composition axes. Mixing both concerns in one package would
blur the responsibility boundary and make each harder to reason about independently.

## NOTE-TC-002: isBlockDataKey Uses Byte-Scan Not Regexp

Regexp compilation (even cached via `sync.Once`) involves a mutex under high concurrency.
The block key pattern (`/block/` followed by decimal digits) is simple enough that a byte-scan
loop is exact, zero-alloc, and O(len(key)) with no lock contention. For keys ≤ 200 bytes
(realistic for any blockpack key), this is strictly cheaper than regexp at scale.

## NOTE-TC-003: strings.LastIndex for /block/ Segment

`strings.LastIndex` (not `strings.Contains` or `strings.Index`) is used to find the `/block/`
segment. This ensures that a fileID containing `/block/` as an internal path component is not
misclassified. Only the trailing segment determines routing. For example:
- `"s3://bucket/block/data/footer"` — last `/block/` tail is `"data/footer"` (not digits → false)
- `"s3://bucket/key/block/42"` — last `/block/` tail is `"42"` (all digits → true)

## NOTE-TC-004: Key Classification Stability

The block data key format (`fileID+"/block/"+blockIdx`) is generated in exactly one place in
the reader (`reader.go:ReadGroup`, SPEC-011). Any new block-data key format must be classified
in `tieredcache_test.go:TestIsBlockDataKey` before merging. This test serves as the
classification registry, ensuring routing is reviewed explicitly for every new key format.
