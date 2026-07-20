package backendworker

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfig_RegisterFlagsAndApplyDefaults_ValueIndexPrefixDefaultsToIndexes
// pins issue #522's catalog_reconcile "vi" branch dependency: an unset
// ValueIndexPrefix must default to "indexes", matching
// valueindexcompactor.DefaultIndexPrefix.
func TestConfig_RegisterFlagsAndApplyDefaults_ValueIndexPrefixDefaultsToIndexes(t *testing.T) {
	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("backendworker", flag.NewFlagSet("", flag.PanicOnError))
	require.Equal(t, "indexes", cfg.ValueIndexPrefix)
}

// TestConfig_RegisterFlagsAndApplyDefaults_ExplicitValueIndexPrefixPreserved
// proves a deployment-configured non-default prefix is never overwritten.
func TestConfig_RegisterFlagsAndApplyDefaults_ExplicitValueIndexPrefixPreserved(t *testing.T) {
	cfg := Config{ValueIndexPrefix: "custom-prefix"}
	cfg.RegisterFlagsAndApplyDefaults("backendworker", flag.NewFlagSet("", flag.PanicOnError))
	require.Equal(t, "custom-prefix", cfg.ValueIndexPrefix)
}
