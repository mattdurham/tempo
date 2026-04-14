package shared

import "testing"

func TestColumnTypeVectorF32Value(t *testing.T) {
	if int(ColumnTypeVectorF32) != 13 {
		t.Errorf("ColumnTypeVectorF32 = %d, want 13", ColumnTypeVectorF32)
	}
	// Ensure distinct from all existing types.
	existing := []ColumnType{
		ColumnTypeString, ColumnTypeInt64, ColumnTypeUint64, ColumnTypeFloat64,
		ColumnTypeBool, ColumnTypeBytes, ColumnTypeRangeInt64, ColumnTypeRangeUint64,
		ColumnTypeRangeDuration, ColumnTypeRangeFloat64, ColumnTypeRangeBytes,
		ColumnTypeRangeString, ColumnTypeUUID,
	}
	for _, ct := range existing {
		if ct == ColumnTypeVectorF32 {
			t.Errorf("ColumnTypeVectorF32 collides with existing type %d", ct)
		}
	}
}

func TestFooterV5Constants(t *testing.T) {
	// FooterV5Size must be 46: V4(34) + vectorOffset[8] + vectorLen[4]
	if FooterV5Size != 46 {
		t.Errorf("FooterV5Size = %d, want 46", FooterV5Size)
	}
	if FooterV5Version != 5 {
		t.Errorf("FooterV5Version = %d, want 5", FooterV5Version)
	}
}

func TestVectorIndexConstants(t *testing.T) {
	if VectorIndexMagic != 0x56454349 {
		t.Errorf("VectorIndexMagic = 0x%X, want 0x56454349", VectorIndexMagic)
	}
	if VectorIndexVersion != 0x01 {
		t.Errorf("VectorIndexVersion = %d, want 1", VectorIndexVersion)
	}
}

func TestWellKnownVectorColumnNames(t *testing.T) {
	if EmbeddingColumnName != "__embedding__" {
		t.Errorf("EmbeddingColumnName = %q, want \"__embedding__\"", EmbeddingColumnName)
	}
	if EmbeddingTextColumnName != "__embedding_text__" {
		t.Errorf("EmbeddingTextColumnName = %q, want \"__embedding_text__\"", EmbeddingTextColumnName)
	}
}
