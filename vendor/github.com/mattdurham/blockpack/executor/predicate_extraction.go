package executor

import (
	"github.com/mattdurham/blockpack/vm"
)

// QueryPredicates is an alias for vm.QueryPredicates for backwards compatibility
type QueryPredicates = vm.QueryPredicates

// isDedicatedColumn checks if an attribute name is a dedicated column
// that has special handling and indexing
