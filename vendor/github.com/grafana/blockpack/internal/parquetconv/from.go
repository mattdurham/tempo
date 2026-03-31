// Package parquetconv converts Tempo vparquet5 block directories to blockpack format.
// This package imports github.com/grafana/tempo/tempodb/encoding and must NOT be
// imported by the main blockpack package (api.go) — doing so creates an import cycle
// because Tempo itself imports github.com/grafana/blockpack.
package parquetconv

import (
	"context"
	"fmt"
	"io"
	"math"
	"path/filepath"

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grafana/blockpack/internal/otlpconvert"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	tempoencoding "github.com/grafana/tempo/tempodb/encoding"
	tempocommon "github.com/grafana/tempo/tempodb/encoding/common"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// ConvertFromParquetBlock reads all traces from a Tempo vparquet5 block directory
// and writes blockpack-formatted trace data to the output writer.
// blockPath must be the path to a Tempo block directory structured as
// {baseDir}/{tenant}/{blockID}, where blockID is a UUID.
// Note: all traces are loaded into memory during conversion; large blocks
// may require substantial memory proportional to their total span data size.
func ConvertFromParquetBlock(blockPath string, output io.Writer, maxSpansPerBlock int) error {
	baseDir, tenant, blockID, err := parseTempoBlockPath(blockPath)
	if err != nil {
		return err
	}

	rawReader, _, _, err := local.New(&local.Config{Path: baseDir})
	if err != nil {
		return fmt.Errorf("open local backend at %q: %w", baseDir, err)
	}
	bReader := backend.NewReader(rawReader)

	ctx := context.Background()

	meta, err := bReader.BlockMeta(ctx, blockID, tenant)
	if err != nil {
		return fmt.Errorf("read block meta for %s/%s: %w", tenant, blockID, err)
	}

	block, err := tempoencoding.OpenBlock(meta, bReader)
	if err != nil {
		return fmt.Errorf("open parquet block: %w", err)
	}

	opts := tempocommon.DefaultSearchOptions()
	searchResp, err := block.Search(ctx, &tempopb.SearchRequest{
		Query: "",
		Limit: math.MaxUint32,
	}, opts)
	if err != nil {
		return fmt.Errorf("search block for trace IDs: %w", err)
	}
	if searchResp == nil {
		return nil
	}

	allTracesData, err := fetchTracesFromBlock(ctx, block, searchResp.Traces, opts)
	if err != nil {
		return fmt.Errorf("fetch traces from block: %w", err)
	}

	blockData, err := otlpconvert.WriteBlockpack(allTracesData, maxSpansPerBlock)
	if err != nil {
		return fmt.Errorf("write blockpack: %w", err)
	}
	if _, err = output.Write(blockData); err != nil {
		return fmt.Errorf("write blockpack output: %w", err)
	}
	return nil
}

// parseTempoBlockPath parses a Tempo block path of the form {baseDir}/{tenant}/{blockID}.
func parseTempoBlockPath(
	blockPath string,
) (baseDir, tenant string, blockID uuid.UUID, err error) {
	absPath, err := filepath.Abs(blockPath)
	if err != nil {
		return "", "", uuid.UUID{}, fmt.Errorf("resolve block path %q: %w", blockPath, err)
	}
	blockIDStr := filepath.Base(absPath)
	tenantDir := filepath.Dir(absPath)
	tenant = filepath.Base(tenantDir)
	baseDir = filepath.Dir(tenantDir)

	blockID, err = uuid.Parse(blockIDStr)
	if err != nil {
		return "", "", uuid.UUID{}, fmt.Errorf(
			"last path component %q is not a valid block UUID: %w", blockIDStr, err,
		)
	}
	return baseDir, tenant, blockID, nil
}

// fetchTracesFromBlock fetches the full trace data for each trace in traceMetas.
func fetchTracesFromBlock(
	ctx context.Context,
	block tempocommon.BackendBlock,
	traceMetas []*tempopb.TraceSearchMetadata,
	opts tempocommon.SearchOptions,
) ([]*tracev1.TracesData, error) {
	allTracesData := make([]*tracev1.TracesData, 0, len(traceMetas))
	for _, tm := range traceMetas {
		if tm == nil {
			continue
		}
		//nolint:staticcheck // util.HexStringToTraceID handles odd-length hex strings from Tempo (leading zeros stripped)
		traceID, err := util.HexStringToTraceID(tm.TraceID)
		if err != nil {
			return nil, fmt.Errorf("decode trace ID %q: %w", tm.TraceID, err)
		}
		traceResp, err := block.FindTraceByID(ctx, traceID, opts)
		if err != nil {
			return nil, fmt.Errorf("find trace %q: %w", tm.TraceID, err)
		}
		if traceResp == nil || traceResp.Trace == nil {
			continue
		}
		td, err := tempoTraceToOTLP(traceResp.Trace)
		if err != nil {
			return nil, fmt.Errorf("convert trace %q to OTLP: %w", tm.TraceID, err)
		}
		allTracesData = append(allTracesData, td)
	}
	return allTracesData, nil
}

// tempoTraceToOTLP converts a *tempopb.Trace to *tracev1.TracesData.
// Tempo and OTLP trace protos share the same wire format — conversion is done
// by marshaling with gogo protobuf (Tempo's library) then unmarshaling with
// google protobuf v2 (OTLP's library).
//
// NOTE-002: This marshal→unmarshal bridge is required because gogo/protobuf (Tempo's
// codegen) and google/protobuf (OTLP's codegen) produce incompatible Go types even
// though they share the same binary wire format. Direct field copy would be fragile
// across Tempo version bumps. See parquetconv/NOTES.md NOTE-002 for full rationale.
func tempoTraceToOTLP(trace *tempopb.Trace) (*tracev1.TracesData, error) {
	td := &tracev1.TracesData{
		ResourceSpans: make([]*tracev1.ResourceSpans, 0, len(trace.ResourceSpans)),
	}
	for _, rs := range trace.ResourceSpans {
		if rs == nil {
			continue
		}
		rsBytes, err := gogoproto.Marshal(rs)
		if err != nil {
			return nil, fmt.Errorf("marshal resource spans: %w", err)
		}
		var otlpRS tracev1.ResourceSpans
		unmarshalErr := proto.Unmarshal(rsBytes, &otlpRS)
		if unmarshalErr != nil {
			return nil, fmt.Errorf("unmarshal resource spans as OTLP: %w", unmarshalErr)
		}
		td.ResourceSpans = append(td.ResourceSpans, &otlpRS)
	}
	return td, nil
}
