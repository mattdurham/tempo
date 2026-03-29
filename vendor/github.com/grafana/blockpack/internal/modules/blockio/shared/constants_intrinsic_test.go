package shared

import "testing"

func TestIntrinsicPageTOCVersion2(t *testing.T) {
	if IntrinsicPageTOCVersion2 != 0x02 {
		t.Errorf("IntrinsicPageTOCVersion2 = 0x%02x, want 0x02", IntrinsicPageTOCVersion2)
	}
}

func TestFooterV4Constants(t *testing.T) {
	// FooterV4Size must be 34: version[2] + headerOffset[8] + compactOffset[8]
	// + compactLen[4] + intrinsicIndexOffset[8] + intrinsicIndexLen[4]
	if FooterV4Size != 34 {
		t.Errorf("FooterV4Size = %d, want 34", FooterV4Size)
	}
	if FooterV4Version != 4 {
		t.Errorf("FooterV4Version = %d, want 4", FooterV4Version)
	}
	if IntrinsicFormatFlat != 0x01 {
		t.Errorf("IntrinsicFormatFlat = %d, want 1", IntrinsicFormatFlat)
	}
	if IntrinsicFormatDict != 0x02 {
		t.Errorf("IntrinsicFormatDict = %d, want 2", IntrinsicFormatDict)
	}
}
