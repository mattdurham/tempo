package shared

// CoalesceConfig is a blockpack data type.
type CoalesceConfig struct {
	MaxGapBytes   int64
	MaxWasteRatio float64
	MaxReadBytes  int64
}
