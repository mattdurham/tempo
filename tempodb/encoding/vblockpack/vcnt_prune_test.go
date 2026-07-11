package vblockpack

// vcnt_prune_test.go — pure unit tests for VCNTFileOverlapsRange (issue #495), covering
// in-range, out-of-range, v1-shaped/malformed always-fetch fallback, and both boundary edges.

import (
	"testing"

	blockpack "github.com/grafana/blockpack"
)

func TestVCNTFileOverlapsRange_V2InRangeReturnsTrue(t *testing.T) {
	name := blockpack.VCNTFormatFilenameV2(0, 100, 200, "id1")
	if !VCNTFileOverlapsRange(name, 150, 160) {
		t.Fatalf("expected overlap for window [150,160] against file range [100,200]")
	}
}

func TestVCNTFileOverlapsRange_V2OutOfRangeReturnsFalse(t *testing.T) {
	name := blockpack.VCNTFormatFilenameV2(0, 100, 200, "id1")
	if VCNTFileOverlapsRange(name, 300, 400) {
		t.Fatalf("expected no overlap for window [300,400] against file range [100,200]")
	}
}

func TestVCNTFileOverlapsRange_V1ShapedAlwaysTrue(t *testing.T) {
	name := blockpack.VCNTFormatFilename(0, "id1")
	if !VCNTFileOverlapsRange(name, 300, 400) {
		t.Fatalf("v1-shaped filename (unknown range) must always be fetched")
	}
}

func TestVCNTFileOverlapsRange_MalformedNameAlwaysTrue(t *testing.T) {
	for _, name := range []string{"", "junk.tmp"} {
		if !VCNTFileOverlapsRange(name, 0, 100) {
			t.Fatalf("malformed filename %q (unknown range) must always be fetched", name)
		}
	}
}

func TestVCNTFileOverlapsRange_BoundaryWallMaxEqualsQueryMin(t *testing.T) {
	name := blockpack.VCNTFormatFilenameV2(0, 50, 100, "id1")
	if !VCNTFileOverlapsRange(name, 100, 200) {
		t.Fatalf("expected overlap when query window starts exactly at file's WallMaxSec")
	}
}

func TestVCNTFileOverlapsRange_BoundaryWallMinEqualsQueryMax(t *testing.T) {
	name := blockpack.VCNTFormatFilenameV2(0, 100, 150, "id1")
	if !VCNTFileOverlapsRange(name, 0, 100) {
		t.Fatalf("expected overlap when query window ends exactly at file's WallMinSec")
	}
}

func TestVCNTFileOverlapsRange_JustOutsideBoundaryReturnsFalse(t *testing.T) {
	name := blockpack.VCNTFormatFilenameV2(0, 100, 150, "id1")
	if VCNTFileOverlapsRange(name, 0, 99) {
		t.Fatalf("expected no overlap when query window ends one second before file starts")
	}
}
