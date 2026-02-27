// Copyright 2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package xflag provides flag parsing utilities.
package xflag

import (
	"flag"
)

// Func is like [flags.Func], but avoids the need for an init func by allocating
// its own storage for the return value.
func Func[T any](name, usage string, fn func(string) (T, error)) *T {
	v := new(T)
	flag.Func(name, usage, func(s string) (err error) {
		*v, err = fn(s)
		return err
	})
	return v
}
