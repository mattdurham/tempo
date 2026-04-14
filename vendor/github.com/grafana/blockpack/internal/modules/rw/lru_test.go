package rw_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/rw"
)

// --- RW-T-06: SharedLRUCache ---

func TestSharedLRUCache_MissReturnsNotFound(t *testing.T) {
	c := rw.NewSharedLRUCache(1024)
	dst := make([]byte, 10)
	assert.False(t, c.Get("file1", 0, dst))
}

func TestSharedLRUCache_PutAndGet(t *testing.T) {
	c := rw.NewSharedLRUCache(1024)
	data := []byte("hello world")
	c.Put("file1", 0, append([]byte(nil), data...), rw.DataTypeBlock) // Put takes ownership
	dst := make([]byte, len(data))
	require.True(t, c.Get("file1", 0, dst))
	assert.Equal(t, data, dst)
}

func TestSharedLRUCache_GetCopiesIntoDst(t *testing.T) {
	c := rw.NewSharedLRUCache(1024)
	original := []byte("original")
	c.Put("file1", 0, append([]byte(nil), original...), rw.DataTypeBlock)

	dst := make([]byte, len(original))
	require.True(t, c.Get("file1", 0, dst))

	// Mutating dst must not corrupt the cache — Get copies into caller's buffer.
	dst[0] = 'X'
	dst2 := make([]byte, len(original))
	require.True(t, c.Get("file1", 0, dst2))
	assert.Equal(t, original, dst2, "cache must return unmodified data after caller mutates dst")
}

func TestSharedLRUCache_DifferentReaderIDsAreIsolated(t *testing.T) {
	c := rw.NewSharedLRUCache(1024)
	c.Put("file1", 0, []byte("aaa"), rw.DataTypeBlock)
	c.Put("file2", 0, []byte("bbb"), rw.DataTypeBlock)

	dst1, dst2 := make([]byte, 3), make([]byte, 3)
	require.True(t, c.Get("file1", 0, dst1))
	require.True(t, c.Get("file2", 0, dst2))
	assert.Equal(t, []byte("aaa"), dst1)
	assert.Equal(t, []byte("bbb"), dst2)
}

func TestSharedLRUCache_EntryLargerThanCapacityIsDropped(t *testing.T) {
	c := rw.NewSharedLRUCache(5) // only 5 bytes
	c.Put("f", 0, make([]byte, 10), rw.DataTypeBlock)
	dst := make([]byte, 10)
	assert.False(t, c.Get("f", 0, dst), "entry larger than maxBytes must not be stored")
}

// TestSharedLRUCache_EvictsBlocksBeforeHighPriorityData verifies that block entries
// are evicted before footer entries when the cache is full.
func TestSharedLRUCache_EvictsBlocksBeforeHighPriorityData(t *testing.T) {
	const entrySize = 10
	// Capacity for exactly 2 entries.
	c := rw.NewSharedLRUCache(2 * entrySize)

	footerData := make([]byte, entrySize)
	blockData1 := make([]byte, entrySize)
	blockData2 := make([]byte, entrySize)
	for i := range footerData {
		footerData[i] = 0xF0
		blockData1[i] = 0xB1
		blockData2[i] = 0xB2
	}

	// Fill cache: 1 footer + 1 block.
	c.Put("f", 0, footerData, rw.DataTypeFooter)
	c.Put("f", 10, blockData1, rw.DataTypeBlock)

	// Add a second block — cache is full so blockData1 (lowest priority) must be evicted.
	c.Put("f", 20, blockData2, rw.DataTypeBlock)

	dstF, dst1, dst2 := make([]byte, entrySize), make([]byte, entrySize), make([]byte, entrySize)
	footerOK := c.Get("f", 0, dstF)
	block1OK := c.Get("f", 10, dst1)
	block2OK := c.Get("f", 20, dst2)

	assert.True(t, footerOK, "footer must survive eviction pressure from block entries")
	assert.False(t, block1OK, "oldest block entry must be evicted first")
	assert.True(t, block2OK, "newly added block entry must be present")
}

// TestSharedLRUCache_PriorityTierOrder verifies the full eviction order:
// Block → TimestampIndex → TraceBloomFilter → Footer.
func TestSharedLRUCache_PriorityTierOrder(t *testing.T) {
	const sz = 10
	c := rw.NewSharedLRUCache(4 * sz)

	footerD := make([]byte, sz)
	bloomD := make([]byte, sz)
	tsD := make([]byte, sz)
	blockD := make([]byte, sz)
	for i := range footerD {
		footerD[i], bloomD[i], tsD[i], blockD[i] = 0xF0, 0xB0, 0x70, 0xC0
	}

	c.Put("f", 0, footerD, rw.DataTypeFooter)
	c.Put("f", 10, bloomD, rw.DataTypeTraceBloomFilter)
	c.Put("f", 20, tsD, rw.DataTypeTimestampIndex)
	c.Put("f", 30, blockD, rw.DataTypeBlock)

	// Cache is at capacity; add one more entry to trigger eviction.
	extra := make([]byte, sz)
	c.Put("f", 40, extra, rw.DataTypeBlock)

	dstF2, dstBl, dstTS, dstBk, dstEx := make(
		[]byte,
		sz,
	), make(
		[]byte,
		sz,
	), make(
		[]byte,
		sz,
	), make(
		[]byte,
		sz,
	), make(
		[]byte,
		sz,
	)
	footerOK := c.Get("f", 0, dstF2)
	bloomOK := c.Get("f", 10, dstBl)
	tsOK := c.Get("f", 20, dstTS)
	blockOK := c.Get("f", 30, dstBk)
	extraOK := c.Get("f", 40, dstEx)

	assert.True(t, footerOK, "footer must not be evicted")
	assert.True(t, bloomOK, "bloom filter must not be evicted")
	assert.True(t, tsOK, "timestamp index must not be evicted")
	assert.False(t, blockOK, "block entry must be evicted (lowest priority)")
	assert.True(t, extraOK, "newly inserted entry must be present")
}

// TestSharedLRUCache_ConcurrentAccess verifies that SharedLRUCache is safe for
// concurrent reads and writes (race detector will catch violations).
func TestSharedLRUCache_ConcurrentAccess(t *testing.T) {
	c := rw.NewSharedLRUCache(64 * 1024)
	const goroutines = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		g := g
		go func() {
			defer wg.Done()
			readerID := fmt.Sprintf("file%d", g%4)
			data := make([]byte, 64)
			c.Put(readerID, int64(g*64), data, rw.DataTypeBlock)
			dst := make([]byte, 64)
			c.Get(readerID, int64(g*64), dst)
		}()
	}

	wg.Wait()
}

// --- RW-T-07: SharedLRUProvider ---

func TestSharedLRUProvider_CacheMissReadsUnderlying(t *testing.T) {
	mem := &memProvider{data: []byte("abcdefghij")}
	cache := rw.NewSharedLRUCache(1024)
	p := rw.NewSharedLRUProvider(mem, "file1", cache)

	buf := make([]byte, 5)
	n, err := p.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("abcde"), buf)
	assert.Equal(t, 1, mem.ReadCount())
}

func TestSharedLRUProvider_CacheHitSkipsUnderlying(t *testing.T) {
	mem := &memProvider{data: []byte("abcdefghij")}
	cache := rw.NewSharedLRUCache(1024)
	p := rw.NewSharedLRUProvider(mem, "file1", cache)

	buf := make([]byte, 5)
	_, err := p.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)

	buf2 := make([]byte, 5)
	n, err := p.ReadAt(buf2, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("abcde"), buf2)
	assert.Equal(t, 1, mem.ReadCount(), "second read must be served from cache, not underlying")
}

func TestSharedLRUProvider_SharedCacheAcrossProviders(t *testing.T) {
	data := []byte("shared data here")
	mem1 := &memProvider{data: data}
	mem2 := &memProvider{data: data}

	cache := rw.NewSharedLRUCache(1024)
	p1 := rw.NewSharedLRUProvider(mem1, "file1", cache)
	p2 := rw.NewSharedLRUProvider(mem2, "file2", cache)

	buf := make([]byte, len(data))

	// Read through p1 — primes cache for file1.
	_, err := p1.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)

	// Read through p2 — different readerID, must miss cache and read from mem2.
	_, err = p2.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)

	assert.Equal(t, 1, mem1.ReadCount(), "file1 should have one real read")
	assert.Equal(t, 1, mem2.ReadCount(), "file2 must read from underlying (different readerID)")

	// Read through p1 again — must hit cache.
	_, err = p1.ReadAt(buf, 0, rw.DataTypeBlock)
	require.NoError(t, err)
	assert.Equal(t, 1, mem1.ReadCount(), "repeated file1 read must be served from cache")
}

func TestSharedLRUProvider_SizeDelegates(t *testing.T) {
	mem := &memProvider{data: make([]byte, 42)}
	cache := rw.NewSharedLRUCache(1024)
	p := rw.NewSharedLRUProvider(mem, "f", cache)
	sz, err := p.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(42), sz)
}

// TestSharedLRUCache_KeyUsesFullIntLength verifies that two Put calls with the same offset
// but different lengths produce independent cache entries — length is a discriminating key
// component. This is a regression test for BUG-15 (int32 truncation of length field).
// RW-T-09.
//
// This test only checks that the length field participates in the cache key; it does not
// exercise the int32-truncation behavior of BUG-15, which only manifests for lengths larger
// than math.MaxInt32 (values that cannot be constructed in a practical unit test). Any two
// unequal small lengths (100 and 200 bytes here) are sufficient to assert that distinct
// lengths at the same offset yield distinct cached entries.
func TestSharedLRUCache_KeyUsesFullIntLength(t *testing.T) {
	c := rw.NewSharedLRUCache(1024)

	data100 := make([]byte, 100)
	data200 := make([]byte, 200)
	for i := range data100 {
		data100[i] = 0xAA
	}
	for i := range data200 {
		data200[i] = 0xBB
	}

	c.Put("f", 0, append([]byte(nil), data100...), rw.DataTypeBlock)
	c.Put("f", 0, append([]byte(nil), data200...), rw.DataTypeBlock)

	dst100 := make([]byte, 100)
	dst200 := make([]byte, 200)

	require.True(t, c.Get("f", 0, dst100), "100-byte entry must hit")
	require.True(t, c.Get("f", 0, dst200), "200-byte entry must hit")

	assert.Equal(t, data100, dst100, "100-byte Get must return 100-byte data")
	assert.Equal(t, data200, dst200, "200-byte Get must return 200-byte data")
}

// TestSharedLRUCache_KeyUsesFullIntLength_LargeValues verifies that cacheKey correctly
// distinguishes lengths that share the same lower 16 bits but differ in higher bits.
// RW-T-09 (extended).
func TestSharedLRUCache_KeyUsesFullIntLength_LargeValues(t *testing.T) {
	// Verify that the cache key uses the full length value, not just lower bits.
	// We construct two slices whose lengths share the same lower 16 bits but differ
	// in higher bits, and ensure they are treated as distinct entries in the cache.
	//
	// This complements TestSharedLRUCache_KeyUsesFullIntLength by exercising a
	// moderately large length, while still being small enough for a unit test.
	// The important property is that the two lengths are different, even though
	// their low-order bits coincide, so any truncation of the length would cause
	// them to collide.
	const base = 1<<16 + 100 // 65636: same lower 16 bits as 100, but different full value
	data1 := make([]byte, 100)
	data2 := make([]byte, base)
	for i := range data1 {
		data1[i] = 0xCC
	}
	for i := range data2 {
		data2[i] = 0xDD
	}

	// Cache capacity must fit both entries.
	c := rw.NewSharedLRUCache(int64(len(data1) + len(data2) + 1024))

	c.Put("g", 0, append([]byte(nil), data1...), rw.DataTypeBlock)
	c.Put("g", 0, append([]byte(nil), data2...), rw.DataTypeBlock)

	dst1 := make([]byte, 100)
	dst2 := make([]byte, base)

	require.True(t, c.Get("g", 0, dst1), "100-byte entry must hit")
	require.True(t, c.Get("g", 0, dst2), "large entry must hit")

	assert.Equal(t, data1, dst1, "100-byte Get must return original data")
	assert.Equal(t, data2, dst2, "large Get must return original data")
}

// TestSharedLRUProvider_ShortReadBecomesErrUnexpectedEOF verifies that a short read
// from the underlying provider (n < len(p), err == nil) is escalated to
// io.ErrUnexpectedEOF and is NOT cached (a subsequent read must still go to underlying).
func TestSharedLRUProvider_ShortReadBecomesErrUnexpectedEOF(t *testing.T) {
	sp := &shortReadProvider{data: []byte("abcdefghij")} // 10 bytes, returns half on each read
	cache := rw.NewSharedLRUCache(1024)
	p := rw.NewSharedLRUProvider(sp, "f", cache)

	buf := make([]byte, 10)
	n, err := p.ReadAt(buf, 0, rw.DataTypeBlock)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF, "short read must surface as ErrUnexpectedEOF")
	assert.Less(t, n, 10, "n must be less than requested on short read")

	// The partial result must not have been cached — a second read must still hit underlying.
	buf2 := make([]byte, 10)
	_, err2 := p.ReadAt(buf2, 0, rw.DataTypeBlock)
	assert.ErrorIs(t, err2, io.ErrUnexpectedEOF, "uncached short read must error again on retry")
}
