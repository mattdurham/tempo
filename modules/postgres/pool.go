package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool parses cfg.DSN and returns a ready, connected *pgxpool.Pool sized per
// cfg.MaxConns/cfg.ConnectTimeout. The caller owns Close() on the returned pool
// -- it must be wired into the same module-service Stop path other components
// use so it never leaks past process shutdown.
func NewPool(ctx context.Context, cfg *Config) (*pgxpool.Pool, error) {
	cfg.applyDefaults()

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn %s: %w", RedactDSN(cfg.DSN), err)
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: new pool %s: %w", RedactDSN(cfg.DSN), err)
	}
	return pool, nil
}
