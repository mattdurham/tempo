package vm

// AggBucket is a blockpack data type.
type AggBucket struct {
	GroupKey GroupKey
	Sum      float64
	Count    int64
	Rate     float64
	Min      float64
	Max      float64
}
