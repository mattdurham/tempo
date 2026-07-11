package postgres

import "time"

// Config configures the opt-in Postgres backend for viusage/cube registries and
// the file catalog (2026-07-11). Nil (the zero value of *Config on tempodb.Config)
// means "not configured" -- every dispatch point in this plan treats a nil
// *postgres.Config identically to how a nil *redis.Config/*memcached.Config means
// "cache role disabled" today.
type Config struct {
	// DSN is a standard postgres:// connection string (libpq-compatible). Secrets
	// (password) MUST be redacted in any UI/log output per this project's
	// standing coding-philosophy rule -- never log cfg.DSN directly; log only the
	// host/dbname portion via RedactedDSN.
	DSN string `yaml:"dsn"`
	// MaxConns bounds this PROCESS's own pgxpool size. Kept deliberately small by
	// default (brainstorm risk: querier-replica-count x per-process-pool-size must
	// stay well under Postgres's max_connections fleet-wide). Default 4.
	MaxConns int32 `yaml:"max_conns"`
	// ConnectTimeout bounds initial connection establishment. Default 5s.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
}

func (c *Config) applyDefaults() {
	if c.MaxConns <= 0 {
		c.MaxConns = 4
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 5 * time.Second
	}
}
