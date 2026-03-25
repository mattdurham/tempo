// Package filecache provides a disk-backed, size-bounded byte cache using one file
// per entry. Compared to the previous bbolt-backed implementation, file-per-entry
// allows concurrent writes to different entries simultaneously — the OS filesystem
// handles concurrency natively instead of serializing through bbolt's single write lock.
//
// Layout: dir/<2-char-hex-prefix>/<sha256hex>.bin
// File content: [4B magic "BPC1"][4B keyLen LE][key bytes][value bytes]
//
// In-memory state is rebuilt on Open by walking the directory. Eviction is FIFO
// (insertion order): when the total exceeds MaxBytes the oldest entries are removed.
// Concurrent fetch deduplication uses singleflight.
package filecache

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/singleflight"
)

const (
	fileMagic     = "BPC1"
	fileHeaderLen = 8 // 4B magic + 4B keyLen
)

// Config configures the file cache.
type Config struct {
	// Path is the directory that will hold the cache files.
	Path string

	// MaxBytes is the maximum total logical payload bytes (sum of key+value sizes).
	// When exceeded, the oldest entries are evicted until under the limit.
	MaxBytes int64

	// Enabled controls whether the cache is active.
	// If false, Open returns (nil, nil) and all cache operations are no-ops.
	Enabled bool
}

// entry is one in-memory record for a cached file.
type entry struct {
	filename string
	key      string
	order    uint64
	size     int64 // len(key) + len(value)
}

// FileCache is a disk-backed, size-bounded byte cache.
// It is safe for concurrent use across any number of goroutines.
//
// Eviction policy: FIFO by insertion order.
// Concurrent writes to different keys happen in parallel (OS-level).
// Concurrent fetches for the same key are deduplicated via singleflight.
type FileCache struct {
	group    singleflight.Group // mutex (no ptr) then map ptr — starts pointer region
	index    map[string]*entry  // key → entry
	dir      string
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	seq      uint64 // monotonic insertion counter for FIFO ordering
}

// Open opens (or creates) a FileCache rooted at cfg.Path.
// Returns (nil, nil) if cfg.Enabled is false.
func Open(cfg Config) (*FileCache, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.MaxBytes <= 0 {
		return nil, fmt.Errorf("filecache: MaxBytes must be positive, got %d", cfg.MaxBytes)
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("filecache: Path must not be empty")
	}

	if err := os.MkdirAll(cfg.Path, 0o700); err != nil {
		return nil, fmt.Errorf("filecache: mkdir %s: %w", cfg.Path, err)
	}

	c := &FileCache{
		dir:      cfg.Path,
		maxBytes: cfg.MaxBytes,
		index:    make(map[string]*entry),
	}

	// Rebuild in-memory index by walking existing cache files.
	if err := c.load(); err != nil {
		return nil, fmt.Errorf("filecache: load: %w", err)
	}

	return c, nil
}

// load scans dir and rebuilds the in-memory index from existing cache files.
// FIFO ordering after restart is approximated by file mtime: entries modified
// earlier receive a lower order value, preserving approximate insertion order.
// Orphaned .tmp files (from interrupted writes) are deleted on load.
func (c *FileCache) load() error {
	return filepath.WalkDir(c.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		// Remove orphaned temp files left by interrupted writes.
		if filepath.Ext(path) == ".tmp" {
			_ = os.Remove(path)
			return nil
		}

		if filepath.Ext(path) != ".bin" {
			return nil
		}

		key, valueSize, readErr := readFileHeader(path)
		if readErr != nil {
			// Corrupted entry — remove it and continue the walk.
			_ = os.Remove(path)
			return nil //nolint:nilerr
		}

		if valueSize < 0 {
			_ = os.Remove(path)
			return nil
		}

		size := int64(len(key)) + valueSize
		if _, exists := c.index[key]; exists {
			// Duplicate (shouldn't happen); remove the extra file.
			_ = os.Remove(path)
			return nil
		}

		// Use mtime as a proxy for insertion order so that FIFO eviction is
		// approximately correct after a restart (files written earlier have a
		// lower mtime and therefore a lower order value).
		info, statErr := d.Info()
		if statErr != nil {
			_ = os.Remove(path)
			return nil //nolint:nilerr
		}
		orderKey := uint64(info.ModTime().UnixNano()) //nolint:gosec

		// Keep c.seq ahead of all loaded order values so that new entries
		// written after restart always sort after existing ones in FIFO eviction.
		if orderKey >= c.seq {
			c.seq = orderKey + 1
		}
		c.index[key] = &entry{
			filename: path,
			key:      key,
			order:    orderKey,
			size:     size,
		}
		c.curBytes += size
		return nil
	})
}

// Close is a no-op for the file-per-entry implementation (no database handle).
// Safe to call on a nil *FileCache.
func (c *FileCache) Close() error {
	return nil
}

// Get retrieves a cached value by key.
// Returns (nil, false, nil) on a miss.
// The returned slice is an independent copy safe for the caller to modify.
func (c *FileCache) Get(key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}

	c.mu.Lock()
	e, ok := c.index[key]
	c.mu.Unlock()
	if !ok {
		return nil, false, nil
	}

	val, err := readFileValue(e.filename)
	if err != nil {
		// On ENOENT (file deleted externally) treat as a miss and clean up.
		// On other errors (permissions, corruption) also treat as miss — the
		// stale index entry is removed so the next call retriggers the fetch.
		c.mu.Lock()
		if cur, still := c.index[key]; still && cur == e {
			delete(c.index, key)
			c.curBytes -= e.size
		}
		c.mu.Unlock()
		return nil, false, nil
	}
	return val, true, nil
}

// Put stores key→value. Evicts FIFO entries if needed to stay within MaxBytes.
// No-op if the key already exists or the entry exceeds MaxBytes.
//
// The file is written BEFORE the index is updated, so a Get for the same key
// will never see a stale index entry pointing to a non-existent file. Concurrent
// Put calls for different keys write their files in parallel (OS-level concurrency).
func (c *FileCache) Put(key string, value []byte) error {
	if c == nil {
		return nil
	}

	needed := int64(len(key) + len(value))
	if needed > c.maxBytes {
		return nil
	}

	c.mu.Lock()
	if _, exists := c.index[key]; exists {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// Write file BEFORE touching the index. Other goroutines writing different
	// keys proceed concurrently — no global lock held during I/O.
	filename := c.pathFor(key)
	if err := writeFile(filename, key, value); err != nil {
		slog.Debug("filecache: write failed", "key", key, "err", err)
		return nil // non-fatal
	}

	// Update index under lock now that the file is safely on disk.
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[key]; exists {
		// Another goroutine wrote the same key concurrently. Both goroutines wrote
		// identical content to the same deterministic path (last os.Rename wins);
		// the file is correct. Do NOT os.Remove — that would delete the winner's file
		// and leave the winner's index entry pointing to a nonexistent path.
		return nil
	}

	c.evictLocked(needed)

	if c.curBytes+needed > c.maxBytes {
		_ = os.Remove(filename)
		return nil
	}

	c.seq++
	c.index[key] = &entry{filename: filename, key: key, size: needed, order: c.seq}
	c.curBytes += needed
	return nil
}

// evictLocked removes FIFO entries to make room for a new entry of size needed.
// Evicts down to 90% of maxBytes minus needed, so the next several Puts don't
// immediately trigger another eviction round.
// Must be called with c.mu held.
func (c *FileCache) evictLocked(needed int64) {
	if c.curBytes+needed <= c.maxBytes {
		return
	}

	// Target: bring curBytes low enough that curBytes+needed ≤ 90% of maxBytes.
	// This ensures the new entry fits AND we have headroom before the next eviction.
	target := c.maxBytes*9/10 - needed
	if target < 0 {
		target = 0 // entire cache must be cleared for a very large entry
	}

	// Collect entries sorted by insertion order (FIFO = lowest seq first).
	type candidate struct {
		e     *entry
		key   string
		order uint64
	}
	candidates := make([]candidate, 0, len(c.index))
	for k, e := range c.index {
		candidates = append(candidates, candidate{e, k, e.order})
	}
	// Insertion sort (ascending). Cache typically has hundreds of entries; fine.
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && candidates[j].order < candidates[j-1].order; j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}

	// Remove files and update index while holding the lock.
	// Releasing the lock during os.Remove creates a race: a concurrent Put
	// for an evicted key could re-create the file at the same deterministic
	// path, which the deferred os.Remove would then silently delete, leaving
	// the new index entry pointing to a nonexistent file.
	// Cache files are small (~KB to ~MB); the lock hold time is acceptable.
	for _, cand := range candidates {
		if c.curBytes <= target {
			break
		}
		_ = os.Remove(cand.e.filename)
		delete(c.index, cand.key)
		c.curBytes -= cand.e.size
	}
}

// GetOrFetch returns the cached value for key, calling fetch() on a miss.
// Concurrent calls for the same uncached key share a single fetch invocation.
// Safe to call on a nil *FileCache (fetch() is called directly).
func (c *FileCache) GetOrFetch(key string, fetch func() ([]byte, error)) ([]byte, error) {
	if c == nil {
		return fetch()
	}

	if val, ok, err := c.Get(key); err != nil {
		return nil, err
	} else if ok {
		return val, nil
	}

	result, err, _ := c.group.Do(key, func() (any, error) {
		if val, ok, getErr := c.Get(key); getErr != nil {
			return nil, getErr
		} else if ok {
			return val, nil
		}

		fetched, fetchErr := fetch()
		if fetchErr != nil {
			return nil, fetchErr
		}

		if putErr := c.Put(key, fetched); putErr != nil {
			slog.Debug("filecache: Put failed, skipping cache", "err", putErr)
		}
		return fetched, nil
	})
	if err != nil {
		return nil, err
	}

	src, ok := result.([]byte)
	if !ok {
		return nil, fmt.Errorf("filecache: unexpected singleflight result type %T", result)
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}

// pathFor returns the file path for a given key.
// Layout: dir/<2-char-prefix>/<sha256hex>.bin
func (c *FileCache) pathFor(key string) string {
	h := sha256.Sum256([]byte(key))
	hexStr := hex.EncodeToString(h[:]) // named hexStr to avoid shadowing the hex package
	return filepath.Join(c.dir, hexStr[:2], hexStr+".bin")
}

// writeFile writes a cache entry to disk atomically via a uniquely named temp file.
// Using os.CreateTemp (rather than a deterministic "<path>.tmp") avoids a race
// where two goroutines writing the same key would clobber each other's temp file.
// Format: [4B magic][4B keyLen LE][key][value]
func writeFile(path string, key string, value []byte) error {
	// Enforce the same key-length constraint that readFileHeader validates,
	// so entries written now won't be rejected as corrupt on the next restart.
	if len(key) == 0 || len(key) > 4096 {
		return fmt.Errorf("filecache: key length %d out of range [1,4096]", len(key))
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	// Create a uniquely named temp file in the same directory so that the
	// final os.Rename is atomic (same filesystem). The "*.tmp" pattern gives
	// names like "1234567890.tmp", which load() recognizes and cleans up.
	f, err := os.CreateTemp(dir, "*.tmp") //nolint:gosec
	if err != nil {
		return err
	}
	tmp := f.Name()

	header := make([]byte, fileHeaderLen)
	copy(header[:4], fileMagic)
	binary.LittleEndian.PutUint32(header[4:], uint32(len(key))) //nolint:gosec

	if _, err = f.Write(header); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp) //nolint:gosec
		return err
	}
	if _, err = f.Write([]byte(key)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp) //nolint:gosec
		return err
	}
	if _, err = f.Write(value); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp) //nolint:gosec
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp) //nolint:gosec
		return err
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmp) //nolint:gosec
		return err
	}
	return os.Rename(tmp, path) //nolint:gosec
}

// readFileHeader reads the magic, key length, and key from a cache file.
// Returns the key and the size of the value payload.
// Returns an error if the file is too short (valueSize would be negative).
func readFileHeader(path string) (key string, valueSize int64, err error) {
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return "", 0, err
	}
	defer f.Close() //nolint:errcheck

	header := make([]byte, fileHeaderLen)
	if _, err = io.ReadFull(f, header); err != nil {
		return "", 0, fmt.Errorf("short header: %w", err)
	}
	if string(header[:4]) != fileMagic {
		return "", 0, fmt.Errorf("bad magic %q", header[:4])
	}

	keyLen := int(binary.LittleEndian.Uint32(header[4:]))
	if keyLen == 0 || keyLen > 4096 {
		return "", 0, fmt.Errorf("bad key length %d", keyLen)
	}

	keyBytes := make([]byte, keyLen)
	if _, err = io.ReadFull(f, keyBytes); err != nil {
		return "", 0, fmt.Errorf("short key: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	valueSize = info.Size() - int64(fileHeaderLen) - int64(keyLen)
	if valueSize < 0 {
		return "", 0, fmt.Errorf("file too short: negative value size %d", valueSize)
	}
	return string(keyBytes), valueSize, nil
}

// readFileValue reads just the value bytes from a cache file.
func readFileValue(path string) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck

	header := make([]byte, fileHeaderLen)
	if _, err = io.ReadFull(f, header); err != nil {
		return nil, err
	}
	if string(header[:4]) != fileMagic {
		return nil, fmt.Errorf("bad magic")
	}

	keyLen := int(binary.LittleEndian.Uint32(header[4:]))
	if keyLen == 0 || keyLen > 4096 {
		return nil, fmt.Errorf("bad key length %d", keyLen)
	}
	if _, err = f.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		return nil, err
	}

	// Guard against reading an unreasonably large value — a file larger than
	// maxReasonableValueBytes indicates corruption or an invalid cache file.
	const maxReasonableValueBytes = 1 << 30 // 1 GiB
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	remaining := info.Size() - int64(fileHeaderLen) - int64(keyLen)
	if remaining < 0 || remaining > maxReasonableValueBytes {
		return nil, fmt.Errorf("unreasonable value size %d", remaining)
	}

	return io.ReadAll(f)
}
