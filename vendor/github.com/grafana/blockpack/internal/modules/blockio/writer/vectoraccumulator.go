package writer

import "math/rand"

type vectorAccumulator struct {
	rng            *rand.Rand
	blocks         []vectorBlockEntry
	trainingSample [][]float32
	dim            int
	totalVectors   int
}
