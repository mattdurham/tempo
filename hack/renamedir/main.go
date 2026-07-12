// Command renamedir performs a pure os.Rename (single syscall, no recursive
// stat/copy) between two same-filesystem paths. Used by deploy.sh to
// quarantine the stale integration/*/e2e_integration_test*/var directories
// (owned by a different uid, permission-denied for traversal) out of the way
// before `go mod vendor`'s "all" package pattern scan, which otherwise fails
// outright trying to read into them. GNU mv/cp both need to stat/read a
// directory's contents in some code paths (e.g. any cross-device fallback,
// or just to compute progress/attributes) and hit the permission wall even
// though a plain rename(2) on the same filesystem never needs to read the
// child's contents at all.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: renamedir <src> <dst>")
		os.Exit(2)
	}
	if err := os.Rename(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, "renamedir:", err)
		os.Exit(1)
	}
}
