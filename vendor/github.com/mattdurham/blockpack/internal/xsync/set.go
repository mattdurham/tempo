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

package xsync

import (
	"iter"
	"sync"
)

// Set is a strongly-typed wrapper over sync.Map, used as a set.
type Set[K comparable] struct {
	impl sync.Map
}

// Load forwards to [sync.Map.Load].
func (s *Set[K]) Load(k K) bool {
	_, ok := s.impl.Load(k)
	return ok
}

// Store forwards to [sync.Map.Store].
func (s *Set[K]) Store(k K) {
	s.impl.Store(k, nil)
}

// All returns an iterator over the values in this set, using [sync.Map.Range].
func (s *Set[K]) All() iter.Seq[K] {
	return func(yield func(K) bool) {
		s.impl.Range(func(key, _ any) bool {
			return yield(key.(K)) //nolint:errcheck
		})
	}
}
