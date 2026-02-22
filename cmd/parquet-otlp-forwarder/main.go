package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/modules/distributor/forwarder/otlpgrpc"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
)

type arrayFlags []string

func (a *arrayFlags) String() string {
	return strings.Join(*a, ",")
}

func (a *arrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {
	var (
		dir       string
		endpoints arrayFlags
		insecure  bool
		certFile  string
		batchSize int
	)

	flag.StringVar(&dir, "dir", "", "Directory containing Tempo block folders (<uuid>/data.parquet + meta.json)")
	flag.Var(&endpoints, "endpoint", "OTLP gRPC endpoint (repeatable, e.g. localhost:4317)")
	flag.BoolVar(&insecure, "insecure", false, "Disable TLS for OTLP gRPC connections")
	flag.StringVar(&certFile, "cert-file", "", "TLS certificate file for OTLP gRPC connections")
	flag.IntVar(&batchSize, "batch-size", 100, "Number of traces to send per OTLP export batch")
	flag.Parse()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	if len(endpoints) == 0 {
		fmt.Fprintf(os.Stderr, "error: at least one --endpoint must be specified\n")
		flag.Usage()
		os.Exit(1)
	}
	if dir == "" {
		fmt.Fprintf(os.Stderr, "error: --dir must be specified\n")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		level.Info(logger).Log("msg", "received shutdown signal")
		cancel()
	}()

	cfg := otlpgrpc.Config{
		TLS: otlpgrpc.TLSConfig{
			Insecure: insecure,
			CertFile: certFile,
		},
	}
	for _, ep := range endpoints {
		cfg.Endpoints = append(cfg.Endpoints, ep)
	}

	fwd, err := otlpgrpc.NewForwarder(cfg, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create forwarder", "err", err)
		os.Exit(1)
	}
	if err := fwd.Dial(ctx); err != nil {
		level.Error(logger).Log("msg", "failed to dial endpoints", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := fwd.Shutdown(ctx); err != nil {
			level.Error(logger).Log("msg", "shutdown error", "err", err)
		}
	}()

	blocks, err := findBlocks(dir)
	if err != nil {
		level.Error(logger).Log("msg", "failed to find blocks", "err", err)
		os.Exit(1)
	}
	if len(blocks) == 0 {
		level.Warn(logger).Log("msg", "no blocks found", "dir", dir)
		return
	}
	level.Info(logger).Log("msg", "found blocks", "count", len(blocks), "dir", dir)

	totalTraces, totalBatches := 0, 0
	for _, b := range blocks {
		if ctx.Err() != nil {
			break
		}
		n, batches, err := processBlock(ctx, b, fwd, batchSize, logger)
		totalTraces += n
		totalBatches += batches
		if err != nil {
			level.Error(logger).Log("msg", "error processing block", "block", b.parquetPath, "err", err)
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "processed block", "block", filepath.Base(filepath.Dir(b.parquetPath)), "traces", n, "batches", batches)
	}

	level.Info(logger).Log("msg", "done", "total_traces", totalTraces, "total_batches", totalBatches)
}

type blockPaths struct {
	parquetPath string
	metaPath    string
}

// findBlocks scans dir for Tempo block subdirectories. Each block is a UUID-named
// directory containing a data.parquet (or data.parquet_.gstmp) and meta.json.
func findBlocks(dir string) ([]blockPaths, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var blocks []blockPaths
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		blockDir := filepath.Join(dir, e.Name())
		parquetPath := findDataFile(blockDir)
		if parquetPath == "" {
			continue
		}
		blocks = append(blocks, blockPaths{
			parquetPath: parquetPath,
			metaPath:    filepath.Join(blockDir, "meta.json"),
		})
	}
	return blocks, nil
}

// findDataFile returns the path to the parquet data file inside a block directory,
// accepting both data.parquet and data.parquet_.gstmp (gsutil download artifact).
func findDataFile(blockDir string) string {
	for _, name := range []string{"data.parquet", "data.parquet_.gstmp"} {
		p := filepath.Join(blockDir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func processBlock(ctx context.Context, b blockPaths, fwd *otlpgrpc.Forwarder, batchSize int, logger log.Logger) (int, int, error) {
	meta := readBlockMeta(b.metaPath, logger)

	switch meta.Version {
	case "vParquet4":
		return processBlockV4(ctx, b, meta, fwd, batchSize, logger)
	default:
		// vParquet5 and anything unrecognised — fall through to v5
		return processBlockV5(ctx, b, meta, fwd, batchSize, logger)
	}
}

func openParquetFile(path string) (*os.File, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func processBlockV4(ctx context.Context, b blockPaths, meta *backend.BlockMeta, fwd *otlpgrpc.Forwarder, batchSize int, logger log.Logger) (int, int, error) {
	f, size, err := openParquetFile(b.parquetPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	pf, err := parquet.OpenFile(f, size,
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(true),
	)
	if err != nil {
		return 0, 0, fmt.Errorf("open parquet: %w", err)
	}

	r := parquet.NewGenericReader[*vparquet4.Trace](pf)
	defer r.Close()

	unmarshaler := &ptrace.ProtoUnmarshaler{}
	batch := make([]*vparquet4.Trace, 0, batchSize)
	rowBuf := make([]*vparquet4.Trace, batchSize)
	totalTraces, totalBatches := 0, 0

	for {
		if ctx.Err() != nil {
			return totalTraces, totalBatches, ctx.Err()
		}

		n, err := r.Read(rowBuf)
		for i := range n {
			batch = append(batch, rowBuf[i])
			rowBuf[i] = nil
		}

		if len(batch) >= batchSize {
			sent, err2 := sendBatchV4(ctx, batch, meta, fwd, unmarshaler, logger)
			totalTraces += sent
			if sent > 0 {
				totalBatches++
			}
			batch = batch[:0]
			if err2 != nil {
				return totalTraces, totalBatches, err2
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return totalTraces, totalBatches, fmt.Errorf("read rows: %w", err)
		}
	}

	if len(batch) > 0 {
		sent, err := sendBatchV4(ctx, batch, meta, fwd, unmarshaler, logger)
		totalTraces += sent
		if sent > 0 {
			totalBatches++
		}
		if err != nil {
			return totalTraces, totalBatches, err
		}
	}

	return totalTraces, totalBatches, nil
}

func processBlockV5(ctx context.Context, b blockPaths, meta *backend.BlockMeta, fwd *otlpgrpc.Forwarder, batchSize int, logger log.Logger) (int, int, error) {
	f, size, err := openParquetFile(b.parquetPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	pf, err := parquet.OpenFile(f, size,
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(true),
	)
	if err != nil {
		return 0, 0, fmt.Errorf("open parquet: %w", err)
	}

	r := parquet.NewGenericReader[*vparquet5.Trace](pf)
	defer r.Close()

	unmarshaler := &ptrace.ProtoUnmarshaler{}
	batch := make([]*vparquet5.Trace, 0, batchSize)
	rowBuf := make([]*vparquet5.Trace, batchSize)
	totalTraces, totalBatches := 0, 0

	for {
		if ctx.Err() != nil {
			return totalTraces, totalBatches, ctx.Err()
		}

		n, err := r.Read(rowBuf)
		for i := range n {
			batch = append(batch, rowBuf[i])
			rowBuf[i] = nil
		}

		if len(batch) >= batchSize {
			sent, err2 := sendBatchV5(ctx, batch, meta, fwd, unmarshaler, logger)
			totalTraces += sent
			if sent > 0 {
				totalBatches++
			}
			batch = batch[:0]
			if err2 != nil {
				return totalTraces, totalBatches, err2
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return totalTraces, totalBatches, fmt.Errorf("read rows: %w", err)
		}
	}

	if len(batch) > 0 {
		sent, err := sendBatchV5(ctx, batch, meta, fwd, unmarshaler, logger)
		totalTraces += sent
		if sent > 0 {
			totalBatches++
		}
		if err != nil {
			return totalTraces, totalBatches, err
		}
	}

	return totalTraces, totalBatches, nil
}

func readBlockMeta(path string, logger log.Logger) *backend.BlockMeta {
	data, err := os.ReadFile(path)
	if err != nil {
		level.Debug(logger).Log("msg", "no meta.json", "path", path)
		return &backend.BlockMeta{}
	}
	meta := &backend.BlockMeta{}
	if err := json.Unmarshal(data, meta); err != nil {
		level.Warn(logger).Log("msg", "failed to parse meta.json", "path", path, "err", err)
		return &backend.BlockMeta{}
	}
	return meta
}

func sendBatchV4(
	ctx context.Context,
	traces []*vparquet4.Trace,
	meta *backend.BlockMeta,
	fwd *otlpgrpc.Forwarder,
	unmarshaler *ptrace.ProtoUnmarshaler,
	logger log.Logger,
) (int, error) {
	combined := ptrace.NewTraces()

	for _, tr := range traces {
		pbTrace := vparquet4.ParquetTraceToTempopbTrace(meta, tr)
		if pbTrace == nil || len(pbTrace.ResourceSpans) == 0 {
			continue
		}

		b, err := pbTrace.Marshal()
		if err != nil {
			level.Warn(logger).Log("msg", "failed to marshal trace", "err", err)
			continue
		}

		td, err := unmarshaler.UnmarshalTraces(b)
		if err != nil {
			level.Warn(logger).Log("msg", "failed to unmarshal trace as ptrace", "err", err)
			continue
		}

		td.ResourceSpans().MoveAndAppendTo(combined.ResourceSpans())
	}

	if combined.SpanCount() == 0 {
		return 0, nil
	}

	if err := fwd.ForwardTraces(ctx, combined); err != nil {
		return 0, fmt.Errorf("forward traces: %w", err)
	}

	return combined.ResourceSpans().Len(), nil
}

func sendBatchV5(
	ctx context.Context,
	traces []*vparquet5.Trace,
	meta *backend.BlockMeta,
	fwd *otlpgrpc.Forwarder,
	unmarshaler *ptrace.ProtoUnmarshaler,
	logger log.Logger,
) (int, error) {
	combined := ptrace.NewTraces()

	for _, tr := range traces {
		pbTrace := vparquet5.ParquetTraceToTempopbTrace(meta, tr)
		if pbTrace == nil || len(pbTrace.ResourceSpans) == 0 {
			continue
		}

		b, err := pbTrace.Marshal()
		if err != nil {
			level.Warn(logger).Log("msg", "failed to marshal trace", "err", err)
			continue
		}

		td, err := unmarshaler.UnmarshalTraces(b)
		if err != nil {
			level.Warn(logger).Log("msg", "failed to unmarshal trace as ptrace", "err", err)
			continue
		}

		td.ResourceSpans().MoveAndAppendTo(combined.ResourceSpans())
	}

	if combined.SpanCount() == 0 {
		return 0, nil
	}

	if err := fwd.ForwardTraces(ctx, combined); err != nil {
		return 0, fmt.Errorf("forward traces: %w", err)
	}

	return combined.ResourceSpans().Len(), nil
}
