package writer

import "github.com/grafana/blockpack/internal/modules/sketch"

type colSketch struct {
	hll   *sketch.HyperLogLog
	topk  *sketch.TopK
	bloom *sketch.SketchBloom
}
