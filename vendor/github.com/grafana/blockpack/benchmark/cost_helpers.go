package benchmark

import "testing"

// AWS pricing constants (2026 pricing for us-east-1)
//
// Sources:
// - S3 Pricing: https://aws.amazon.com/s3/pricing/
// - Lambda Pricing: https://aws.amazon.com/lambda/pricing/
// - Cost Analysis: https://cloudchipr.com/blog/amazon-s3-pricing-explained
const (
	// S3 GET request cost per 1,000 requests
	s3GetRequestCostPer1000 = 0.0004

	// S3 data transfer cost per GB (first 10 TB tier)
	// Set to 0 for same-region transfers
	s3DataTransferCostPerGB = 0.0

	// Lambda compute cost per GB-second (x86)
	lambdaComputeCostPerGBSecond = 0.0000166667

	// Lambda request cost per 1 million requests
	lambdaRequestCostPerMillion = 0.20

	// Assumed Lambda memory allocation in MB
	// Typical configuration for query workload
	lambdaMemoryMB = 1024
)

// ReportCosts calculates and reports AWS cost metrics for a benchmark
// based on I/O operations, bytes read, and execution time.
//
// This adds the following custom metrics to benchmark output:
//   - cost_s3_get: S3 GET request costs
//   - cost_s3_xfer: S3 data transfer costs (within same region = $0)
//   - cost_lambda_compute: Lambda compute costs (GB-seconds)
//   - cost_lambda_requests: Lambda request costs
//   - cost_total: Total AWS costs (S3 + Lambda)
func ReportCosts(b *testing.B, ioOps int64, bytesRead int64, timeNs int64) {
	// Calculate S3 GET request cost
	s3GetCost := float64(ioOps) / 1000 * s3GetRequestCostPer1000

	// Calculate S3 data transfer cost (within same region = $0)
	bytesReadGB := float64(bytesRead) / (1024 * 1024 * 1024)
	s3TransferCost := bytesReadGB * s3DataTransferCostPerGB

	// Calculate Lambda compute cost
	// GB-seconds = (Memory in MB / 1024) * (Duration in seconds)
	memoryGB := float64(lambdaMemoryMB) / 1024.0
	durationSeconds := float64(timeNs) / 1e9
	gbSeconds := memoryGB * durationSeconds
	lambdaComputeCost := gbSeconds * lambdaComputeCostPerGBSecond

	// Calculate Lambda request cost
	lambdaRequestCost := 1.0 / 1000000.0 * lambdaRequestCostPerMillion

	// Total cost (S3 + Lambda)
	totalCost := s3GetCost + s3TransferCost + lambdaComputeCost + lambdaRequestCost

	// Report cost metrics
	b.ReportMetric(s3GetCost, "cost_s3_get")
	b.ReportMetric(s3TransferCost, "cost_s3_xfer")
	b.ReportMetric(lambdaComputeCost, "cost_lambda_compute")
	b.ReportMetric(lambdaRequestCost, "cost_lambda_requests")
	b.ReportMetric(totalCost, "cost_total")
}
