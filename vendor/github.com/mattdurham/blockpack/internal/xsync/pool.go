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

import "sync"

// Pool is like sync.Pool, but strongly typed to make the interface a bit
// less messy.
type Pool[T any] struct {
	New   func() *T // Called to construct new values.
	Reset func(*T)  // Called to reset values before re-use.

	impl sync.Pool
}

// Get returns a cached value of type T.
//
//go:nosplit
func (p *Pool[T]) Get() *T {
	v, _ := p.impl.Get().(*T) // intentionally ignore ok - type assertion failure returns nil which is handled below
	if v == nil {
		switch p.New {
		case nil:
			v = new(T)
		default:
			v = p.New()
		}
	}
	return v
}

// Put returns a cached value of type T.
//
//go:nosplit
func (p *Pool[T]) Put(v *T) {
	if p.Reset != nil {
		p.Reset(v)
	}
	p.impl.Put(v)
}
