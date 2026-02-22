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

//go:build !debug

// Package debug provides debugging utilities.
package debug

// Enabled indicates whether debugging is enabled.
const Enabled = false

// Log logs a debug message when debugging is enabled.
func Log([]any, string, string, ...any) {}

// Assert asserts a condition and logs a message if it fails.
func Assert(bool, string, ...any) {}

// Value is a debug value wrapper.
type Value[T any] struct {
	_ struct{}
}

// Get retrieves the wrapped debug value.
func (v *Value[T]) Get() *T {
	panic("called Value.Get() when not in debug mode")
}
