package main

type otelDemoRowMetrics struct {
	blockpackTime, parquetTime         string
	blockpackCPU, parquetCPU           string
	blockpackBytes, parquetBytes       string
	blockpackIOOps, parquetIOOps       string
	blockpackMemory, parquetMemory     string
	blockpackAllocs, parquetAllocs     string
	blockpackCostHTML, parquetCostHTML string
	winCell                            string
	cVal, pVal                         OTELDemoResult
	blockpackWins, bothExist           bool
}
