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
	"unsafe"

	"github.com/mattdurham/blockpack/internal/xunsafe/layout"
)

// Cast casts one pointer type to another.
func Cast[To, From any](p *From) *To {
	return (*To)(unsafe.Pointer(p)) //nolint:gosec
}

// Add adds the given offset to p, scaled by the size of T.
func Add[P ~*E, E any, I Int](p P, n I) P {
	size := layout.Size[E]()
	return P(unsafe.Add(unsafe.Pointer(p), uintptr(size)*uintptr(n))) //nolint:gosec
}

// Sub computes the difference between two pointers, scaled by the size of T.
func Sub[P ~*E, E any](p1, p2 P) int {
	size := layout.Size[E]()
	return int(uintptr(unsafe.Pointer(p1))-uintptr(unsafe.Pointer(p2))) / size //nolint:gosec
}

// Load loads a value of the given type at the given index.
func Load[P ~*E, E any, I Int](p P, n I) E {
	return *Add(p, n)
}

// Store stores a value at the given index.
func Store[P ~*E, E any, I Int](p P, n I, v E) {
	*Add(p, n) = v
}

// StoreNoWB performs a store without generating any write barriers.
func StoreNoWB[P ~*E, E any](p *P, q P) {
	*Cast[uintptr](p) = uintptr(unsafe.Pointer(q)) //nolint:gosec
}

// StoreNoWBUntyped performs a store without generating any write barriers.
func StoreNoWBUntyped[P ~unsafe.Pointer](p *P, q P) {
	*Cast[uintptr](p) = uintptr(q)
}

// Copy copies n elements from one pointer to the other.
func Copy[P ~*E, E any, I Int](dst, src P, n I) {
	copy(unsafe.Slice(dst, n), unsafe.Slice(src, n)) //nolint:gosec
}

// Clear zeros n elements at p.
func Clear[P ~*E, E any, I Int](p P, n I) {
	clear(unsafe.Slice(p, n)) //nolint:gosec
}
