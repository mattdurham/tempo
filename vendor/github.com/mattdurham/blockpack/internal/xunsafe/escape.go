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

import "unsafe"

var (
	alwaysFalse bool
	sink        unsafe.Pointer //nolint:unused
)

// Escape escapes a pointer to the heap.
func Escape[P ~*E, E any](p P) P {
	if alwaysFalse {
		sink = unsafe.Pointer(p)
	}
	return p
}

// NoEscape hides a pointer from escape analysis, preventing it from
// escaping to the heap.
func NoEscape[P ~*E, E any](p P) P {
	//nolint:staticcheck // False positive: complains that p^0 does nothing.
	return P((AddrOf(p) ^ 0).AssertValid())
}
