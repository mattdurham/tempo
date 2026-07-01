// Package main is a debug utility for inspecting vector column encoding in blockpack files.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/blockio/writer"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

func (m *mp) Size() (int64, error) { return int64(len(m.data)), nil }
func (m *mp) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	return copy(p, m.data[off:]), nil
}

func main() {
	const dim = 4
	var buf bytes.Buffer
	w, wErr := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 5, VectorDimension: dim})
	if wErr != nil {
		panic(wErr)
	}

	for i := range 3 {
		traceID := make([]byte, 16)
		traceID[15] = byte(i + 1) //nolint:gosec // i bounded by range 3
		spanID := make([]byte, 8)
		spanID[7] = byte(i + 1) //nolint:gosec // i bounded by range 3
		vec := []float32{float32(i), float32(i) + 1, float32(i) + 2, float32(i) + 3}
		bytesVec := make([]byte, dim*4)
		for j, f := range vec {
			binary.LittleEndian.PutUint32(bytesVec[j*4:], math.Float32bits(f))
		}
		span := &tracev1.Span{
			TraceId: traceID, SpanId: spanID, Name: "op",
			StartTimeUnixNano: 1000, EndTimeUnixNano: 2000,
			Attributes: []*commonv1.KeyValue{{
				Key:   modules_shared.EmbeddingColumnName,
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: bytesVec}},
			}},
		}
		td := &tracev1.TracesData{ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{
					{
						Key:   "service.name",
						Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
					},
				},
			},
			ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{span}}},
		}}}
		if addErr := w.AddTracesData(td); addErr != nil {
			panic(addErr)
		}
	}
	if _, flushErr := w.Flush(); flushErr != nil {
		panic(flushErr)
	}

	r, rErr := modules_reader.NewReaderFromProvider(&mp{data: buf.Bytes()})
	if rErr != nil {
		panic(rErr)
	}

	fmt.Printf("Block count: %d\n", r.BlockCount())
	raw, rawErr := r.ReadBlockRaw(0)
	if rawErr != nil {
		panic(rawErr)
	}

	// Find embedding column
	spanCount := binary.LittleEndian.Uint32(raw[8:12])
	colCount := int(binary.LittleEndian.Uint32(raw[12:16]))
	fmt.Printf("Block 0: spanCount=%d colCount=%d\n", spanCount, colCount)

	pos := int(modules_shared.BlockHeaderV14Size)
	for j := 0; j < colCount; j++ {
		nameLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		name := string(raw[pos : pos+nameLen])
		pos += nameLen
		colType := raw[pos]
		pos++
		dataOff := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8
		dataLen := binary.LittleEndian.Uint64(raw[pos:])
		pos += 8
		if name == modules_shared.EmbeddingColumnName {
			colData := raw[dataOff : dataOff+dataLen]
			fmt.Printf("colData len=%d, bytes: %X\n", len(colData), colData)
			_ = colType
			// Parse
			if len(colData) >= 2 {
				fmt.Printf("enc_version=%d, kind=%d\n", colData[0], colData[1])
				data := colData[2:]
				if len(data) >= 10 {
					dim2 := binary.LittleEndian.Uint16(data[0:2])
					rowCount := binary.LittleEndian.Uint32(data[2:6])
					rleLen := binary.LittleEndian.Uint32(data[6:10])
					fmt.Printf("dim=%d rowCount=%d rleLen=%d\n", dim2, rowCount, rleLen)
					off := uint32(10) + rleLen
					if int(off)+4 <= len(data) {
						cLen := binary.LittleEndian.Uint32(data[off : off+4])
						fmt.Printf("compressed float_data_len=%d, remaining=%d\n", cLen, len(data)-int(off)-4)
						if int(off)+4+int(cLen) <= len(data) {
							compData := data[off+4 : off+4+cLen]
							fmt.Printf("first 4 bytes of compressed: %X\n", compData[:min(4, len(compData))])
						}
					}
				}
			}
		}
	}
}
