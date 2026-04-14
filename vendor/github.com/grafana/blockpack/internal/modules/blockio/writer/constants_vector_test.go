package writer

import "testing"

func TestKindVectorF32Value(t *testing.T) {
	if KindVectorF32 != 14 {
		t.Errorf("KindVectorF32 = %d, want 14", KindVectorF32)
	}
}
