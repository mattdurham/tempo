package executor

import modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"

type blockGroupReader interface {
	// ReadGroup downloads full bytes for all blocks in cr. Used for WantAll paths (compaction).
	ReadGroup(cr modules_shared.CoalescedRead) (map[int][]byte, error)
	// ReadGroupColumnar downloads only bytes for wantColumns, routing through the section cache.
	// Falls back to ReadGroup when wantColumns is nil or fileID is empty.
	ReadGroupColumnar(cr modules_shared.CoalescedRead, wantColumns map[string]struct{}) (map[int][]byte, error)
	BlockMeta(blockIdx int) modules_shared.BlockMeta
}
