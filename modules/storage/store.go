package storage

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/pkg/cache"
	"github.com/grafana/tempo/pkg/usagestats"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
)

var (
	statCache   = usagestats.NewString("storage_cache")
	statBackend = usagestats.NewString("storage_backend")
)

// Store wraps the tempodb storage layer
type Store interface {
	services.Service

	tempodb.Reader
	tempodb.Writer
	tempodb.Compactor
}

type store struct {
	services.Service

	cfg Config

	tempodb.Reader
	tempodb.Writer
	tempodb.Compactor
}

var (
	_ tempodb.RawReaderProvider = (*store)(nil)
	_ tempodb.PgPoolProvider    = (*store)(nil)
)

// NewStore creates a new Tempo Store using configuration supplied.
func NewStore(cfg Config, cacheProvider cache.Provider, logger log.Logger) (Store, error) {
	statCache.Set(cfg.Trace.Cache)
	statBackend.Set(cfg.Trace.Backend)

	r, w, c, err := tempodb.New(&cfg.Trace, cacheProvider, logger)
	if err != nil {
		return nil, err
	}

	s := &store{
		cfg:       cfg,
		Reader:    r,
		Writer:    w,
		Compactor: c,
	}

	s.Service = services.NewIdleService(s.starting, s.stopping)
	return s, nil
}

// RawReader implements tempodb.RawReaderProvider by forwarding to the underlying Reader's
// own capability. Required because embedding tempodb.Reader here as an interface-typed field
// only promotes methods declared on that interface -- it does not promote RawReader(), even
// though the concrete *readerWriter tempodb.New returns implements it. Without this, every
// reader.(tempodb.RawReaderProvider) assertion against a *store (frontend search/structural/
// metrics sharders) silently fails and their VCNT-fetch/plan-time codepaths never engage.
func (s *store) RawReader() backend.RawReader {
	if rrp, ok := s.Reader.(tempodb.RawReaderProvider); ok {
		return rrp.RawReader()
	}
	return nil
}

// PgPool implements tempodb.PgPoolProvider (issue #522 #157/#161), forwarding to the
// underlying Reader's own capability -- the exact same embedded-interface method-promotion gap
// RawReader immediately above already exists to close, for the same reason: embedding
// tempodb.Reader here as an interface-typed field does not promote PgPool(), even though the
// concrete *readerWriter tempodb.New returns implements it. Without this, every
// reader.(tempodb.PgPoolProvider) assertion against a *store (frontend search/metrics sharders'
// compactedKeyChecker derivation for fetchVCNTSection) silently fails and #157's mandatory VCNT
// compacted-key exclusion filter never engages in production.
func (s *store) PgPool() *pgxpool.Pool {
	if ppp, ok := s.Reader.(tempodb.PgPoolProvider); ok {
		return ppp.PgPool()
	}
	return nil
}

func (s *store) starting(_ context.Context) error {
	return nil
}

func (s *store) stopping(_ error) error {
	s.Reader.Shutdown()

	return nil
}
