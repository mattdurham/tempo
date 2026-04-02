// Package parity provides end-to-end field-coverage smoke tests for the blockpack
// columnar format. These tests write OTLP spans covering all intrinsic fields that
// are captured as block columns and all five attribute value types (string, int64,
// float64, bool, bytes) across span, resource, and scope namespaces, then read back
// and assert all column values match.
//
// TestParitySmokeTest is CI-gated: it is run as a separate step in ci.yml and in the
// Makefile ci target, and is also included in the standard make test run via $(PACKAGES).
package parity
