// Copyright 2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xunsafe

import (
	"math"
	"unsafe"

	"github.com/grafana/blockpack/internal/xunsafe/layout"
)

// BoundsCheck emulates a bounds check on a slice with the given index and
// length.
func BoundsCheck(n, length int) {
	dummy := unsafe.Slice(&struct{}{}, length&^math.MinInt) //nolint:gosec
	_ = dummy[n]
}

// Bytes converts a pointer into a slice of its contents.
func Bytes[P ~*E, E any](p P) []byte {
	size := layout.Size[E]()
	return unsafe.Slice(Cast[byte](p), size) //nolint:gosec
}

// LoadSlice loads a slice without performing a bounds check.
func LoadSlice[S ~[]E, E any, I Int](s S, n I) E {
	return Load(unsafe.SliceData(s), n) //nolint:gosec
}

// SliceToString converts a slice into a string, multiplying the slice length
// as appropriate.
func SliceToString[S ~[]E, E any](s S) string {
	size := layout.Size[E]()
	str := struct {
		ptr *E
		len int
	}{unsafe.SliceData(s), len(s) * size} //nolint:gosec
	return BitCast[string](str)
}

// StringToSlice converts a string into a slice, multiplying the slice length
// as appropriate.
func StringToSlice[S ~[]E, E any](s string) S {
	size := layout.Size[E]()
	return unsafe.Slice(Cast[E](unsafe.StringData(s)), len(s)/size) //nolint:gosec
}
