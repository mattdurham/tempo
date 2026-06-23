package main

type FileSize struct {
	BlockpackMB          float64
	ParquetMB            float64
	RealWorldBlockpackMB float64
	RealWorldParquetMB   float64
	WritePathBlockpackMB float64
	WritePathParquetMB   float64
	BlockpackHotMB       float64
	BlockpackColdMB      float64
}
