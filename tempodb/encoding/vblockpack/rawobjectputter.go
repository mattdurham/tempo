package vblockpack

// rawobjectputter.go — Track C: unconditional Put over backend.RawWriter, the
// backend-agnostic (Local/GCS/Azure) counterpart to valueindex.go's s3ObjectPutter.
// Backs ConfigureValueIndex's valueIndexSink/vcntSink and ConfigureCubeManager's
// L0-file store. Each write uses a fresh, collision-free filename (xid.New()-suffixed
// by its caller), so no conditional-write semantics are needed here — this is a thin
// RawWriter.Write wrapper, nothing more.

import (
	"bytes"
	"context"

	"github.com/grafana/tempo/tempodb/backend"
)

// rawObjectPutter satisfies blockpack.ObjectPutter (Put by full object key) over a
// generic backend.RawWriter.
type rawObjectPutter struct {
	rawW backend.RawWriter
}

func newRawObjectPutter(rawW backend.RawWriter) *rawObjectPutter {
	return &rawObjectPutter{rawW: rawW}
}

// Put writes data to key unconditionally.
func (p *rawObjectPutter) Put(key string, data []byte) error {
	name, keypath := splitKeyForRaw(key)
	return p.rawW.Write(context.Background(), name, keypath, bytes.NewReader(data), int64(len(data)), nil)
}
