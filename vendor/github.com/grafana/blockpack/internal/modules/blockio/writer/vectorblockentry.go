package writer

type vectorBlockEntry struct {
	centroid    []float32
	pqCodes     [][]byte
	vectors     [][]float32
	vectorCount int
}
