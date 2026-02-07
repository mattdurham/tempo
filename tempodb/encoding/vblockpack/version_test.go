package vblockpack

import (
	"testing"

	"github.com/grafana/tempo/tempodb/encoding"
)

func TestVersionString(t *testing.T) {
	if VersionString != "vBlockpack1" {
		t.Errorf("expected VersionString to be vBlockpack1, got %s", VersionString)
	}
}

func TestEncodingImplementsInterface(t *testing.T) {
	var _ encoding.VersionedEncoding = &Encoding{}
}
