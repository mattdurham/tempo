package executor

type aggBucketState struct {
	values []float64
	sum    float64
	count  int64
	min    float64
	max    float64
	mean   float64
	m2     float64
}
