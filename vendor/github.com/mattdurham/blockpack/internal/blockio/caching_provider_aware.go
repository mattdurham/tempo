package blockio

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// CacheRegion identifies different data regions in blockpack files
type CacheRegion int

const (
	RegionUnknown    CacheRegion = iota
	RegionFooter                 // Last 41 bytes - always cached
	RegionMetadata               // Schema and column info - always cached
	RegionColumnData             // Column data - LRU with prefetching
	RegionRowData                // Row data - adaptive based on access pattern
)

func (r CacheRegion) String() string {
	switch r {
	case RegionFooter:
		return "footer"
	case RegionMetadata:
		return "metadata"
	case RegionColumnData:
		return "column_data"
	case RegionRowData:
		return "row_data"
	default:
		return "unknown"
	}
}

// AccessPatternTracker tracks access patterns to determine caching strategy
type AccessPatternTracker struct {
	lastOffset     atomic.Int64
	sequentialRuns atomic.Int64
	randomAccesses atomic.Int64
	totalAccesses  atomic.Int64

	// Heat map tracks hot regions (offset range -> access count)
	heatMap    map[int64]int64
	heatMapMu  sync.RWMutex
	heatWindow int64 // Size of heat map buckets (e.g., 64KB)
}

// NewAccessPatternTracker creates a new access pattern tracker
func NewAccessPatternTracker(heatWindow int64) *AccessPatternTracker {
	if heatWindow <= 0 {
		heatWindow = 64 * 1024 // Default 64KB buckets
	}
	tracker := &AccessPatternTracker{
		heatMap:    make(map[int64]int64),
		heatWindow: heatWindow,
	}
	// Initialize lastOffset to -1 to avoid misclassifying first access at offset 0 as sequential
	tracker.lastOffset.Store(-1)
	return tracker
}

// RecordAccess records an access at the given offset and returns whether it's sequential
func (a *AccessPatternTracker) RecordAccess(offset int64, length int) bool {
	a.totalAccesses.Add(1)

	// Track heat map
	bucket := offset / a.heatWindow
	a.heatMapMu.Lock()
	a.heatMap[bucket]++
	a.heatMapMu.Unlock()

	// Check if access is sequential
	lastOff := a.lastOffset.Load()
	a.lastOffset.Store(offset)

	if lastOff >= 0 && offset >= lastOff && offset-lastOff <= int64(length)*2 {
		// Sequential access (within reasonable range)
		a.sequentialRuns.Add(1)
		return true
	}

	// Random access
	a.randomAccesses.Add(1)
	return false
}

// IsSequential returns true if access pattern is mostly sequential
func (a *AccessPatternTracker) IsSequential() bool {
	total := a.totalAccesses.Load()
	if total < 10 {
		// Not enough data
		return false
	}

	sequential := a.sequentialRuns.Load()
	// Consider sequential if > 60% of accesses are sequential
	return float64(sequential)/float64(total) > 0.6
}

// GetHotRegions returns the N hottest regions
func (a *AccessPatternTracker) GetHotRegions(n int) []int64 {
	a.heatMapMu.RLock()
	defer a.heatMapMu.RUnlock()

	// Sort by access count
	type bucketCount struct {
		bucket int64
		count  int64
	}
	buckets := make([]bucketCount, 0, len(a.heatMap))
	for bucket, count := range a.heatMap {
		buckets = append(buckets, bucketCount{bucket, count})
	}

	// Use sort.Slice for O(n log n) instead of O(n²) bubble sort
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].count > buckets[j].count
	})

	// Return top N bucket offsets
	result := make([]int64, 0, n)
	for i := 0; i < n && i < len(buckets); i++ {
		result = append(result, buckets[i].bucket*a.heatWindow)
	}
	return result
}

// Stats returns access pattern statistics
func (a *AccessPatternTracker) Stats() AccessPatternStats {
	total := a.totalAccesses.Load()
	sequential := a.sequentialRuns.Load()
	random := a.randomAccesses.Load()

	var sequentialPct float64
	if total > 0 {
		sequentialPct = float64(sequential) / float64(total) * 100
	}

	a.heatMapMu.RLock()
	hotRegions := len(a.heatMap)
	a.heatMapMu.RUnlock()

	return AccessPatternStats{
		TotalAccesses:  total,
		SequentialRuns: sequential,
		RandomAccesses: random,
		SequentialPct:  sequentialPct,
		HotRegionCount: hotRegions,
	}
}

// AccessPatternStats contains access pattern statistics
type AccessPatternStats struct {
	TotalAccesses  int64
	SequentialRuns int64
	RandomAccesses int64
	SequentialPct  float64
	HotRegionCount int
}

// DataAwareCachingProvider extends CachingReaderProvider with data-aware caching
type DataAwareCachingProvider struct {
	*CachingReaderProvider

	// Access pattern tracking
	accessTracker *AccessPatternTracker

	// Statistics
	regionHits   map[CacheRegion]int64
	regionMisses map[CacheRegion]int64
	statsMu      sync.RWMutex
}

// NewDataAwareCachingProvider creates a data-aware caching provider
func NewDataAwareCachingProvider(underlying ReaderProvider, config CachingProviderConfig) (*DataAwareCachingProvider, error) {
	base, err := NewCachingReaderProvider(underlying, config)
	if err != nil {
		return nil, err
	}

	return &DataAwareCachingProvider{
		CachingReaderProvider: base,
		accessTracker:         NewAccessPatternTracker(64 * 1024),
		regionHits:            make(map[CacheRegion]int64),
		regionMisses:          make(map[CacheRegion]int64),
	}, nil
}

// ReadAt implements data-aware reading with intelligent caching using explicit data type hints.
// The dataType parameter is provided by the caller (Reader) and eliminates the need for region detection.
func (d *DataAwareCachingProvider) ReadAt(p []byte, off int64, dataType DataType) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Map DataType to CacheRegion
	region := d.dataTypeToRegion(dataType)

	// Record access pattern
	isSequential := d.accessTracker.RecordAccess(off, len(p))

	// Use appropriate caching strategy based on data type
	switch region {
	case RegionFooter, RegionMetadata:
		// Footer and metadata use standard LRU cache
		// They're small and accessed frequently, so they'll naturally stay cached
		return d.readWithLRUCache(p, off, region, dataType)

	case RegionColumnData:
		// LRU cache with prefetching for sequential access
		if isSequential && d.accessTracker.IsSequential() {
			return d.readWithPrefetch(p, off, region, dataType)
		}
		return d.readWithLRUCache(p, off, region, dataType)

	case RegionRowData:
		// Adaptive based on access pattern
		if d.accessTracker.IsSequential() {
			return d.readWithPrefetch(p, off, region, dataType)
		}
		return d.readWithLRUCache(p, off, region, dataType)

	default:
		// Unknown region - use basic caching
		return d.readWithLRUCache(p, off, region, dataType)
	}
}

// dataTypeToRegion maps DataType to internal CacheRegion
func (d *DataAwareCachingProvider) dataTypeToRegion(dataType DataType) CacheRegion {
	switch dataType {
	case DataTypeFooter:
		return RegionFooter
	case DataTypeMetadata:
		return RegionMetadata
	case DataTypeColumn:
		return RegionColumnData
	case DataTypeRow:
		return RegionRowData
	case DataTypeIndex:
		return RegionColumnData // Treat index as column data
	default:
		return RegionColumnData // Default to column data for unknown types
	}
}

// readWithLRUCache uses standard LRU caching
func (d *DataAwareCachingProvider) readWithLRUCache(p []byte, off int64, region CacheRegion, dataType DataType) (int, error) {
	cacheKey := d.CachingReaderProvider.makeCacheKey(off, len(p))

	// Check cache
	if cached, found := d.CachingReaderProvider.cache.Get(cacheKey); found {
		d.recordRegionHit(region)
		n := copy(p, cached)
		// If cached data is shorter than requested, return io.EOF to match io.ReaderAt semantics
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}

	// Cache miss
	d.recordRegionMiss(region)
	n, err := d.CachingReaderProvider.underlying.ReadAt(p, off, dataType)
	if err != nil && n == 0 {
		return 0, err
	}

	// Only cache complete reads to avoid breaking io.ReaderAt semantics
	// Partial reads (n < len(p)) should not be cached
	if n > 0 && n == len(p) && err == nil {
		cached := make([]byte, n)
		copy(cached, p[:n])
		d.CachingReaderProvider.cache.Put(cacheKey, cached)
	}

	return n, err
}

// readWithPrefetch reads with prefetching for sequential access
func (d *DataAwareCachingProvider) readWithPrefetch(p []byte, off int64, region CacheRegion, dataType DataType) (int, error) {
	// For now, just use LRU cache
	// TODO: Implement actual prefetching logic
	return d.readWithLRUCache(p, off, region, dataType)
}

// recordRegionHit records a cache hit for a region
func (d *DataAwareCachingProvider) recordRegionHit(region CacheRegion) {
	d.statsMu.Lock()
	d.regionHits[region]++
	d.statsMu.Unlock()
}

// recordRegionMiss records a cache miss for a region
func (d *DataAwareCachingProvider) recordRegionMiss(region CacheRegion) {
	d.statsMu.Lock()
	d.regionMisses[region]++
	d.statsMu.Unlock()
}

// DataAwareStats returns comprehensive statistics
func (d *DataAwareCachingProvider) DataAwareStats() DataAwareStats {
	baseStats := d.Stats()
	patternStats := d.accessTracker.Stats()

	d.statsMu.RLock()
	regionHits := make(map[string]int64)
	regionMisses := make(map[string]int64)
	for region, count := range d.regionHits {
		regionHits[region.String()] = count
	}
	for region, count := range d.regionMisses {
		regionMisses[region.String()] = count
	}
	d.statsMu.RUnlock()

	return DataAwareStats{
		BaseStats:    baseStats,
		PatternStats: patternStats,
		RegionHits:   regionHits,
		RegionMisses: regionMisses,
	}
}

// DataAwareStats contains comprehensive caching statistics
type DataAwareStats struct {
	BaseStats    CacheStats
	PatternStats AccessPatternStats
	RegionHits   map[string]int64
	RegionMisses map[string]int64
}
