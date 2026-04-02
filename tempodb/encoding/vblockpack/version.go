package vblockpack

import (
	"go.opentelemetry.io/otel"
)

const (
	// VersionString identifies the blockpack encoding version
	VersionString = "vblockpack"

	// DataFileName is the name of the blockpack data file
	DataFileName = "data.blockpack"
)

var tracer = otel.Tracer("tempodb/encoding/vblockpack")
