package legacysystem

import (
	"context"
	"sync"

	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/legacysystem/servers"
	"go.uber.org/zap"
)

type SystemOptions struct {
	Logger        *zap.Logger
	DataServer    data_v1.DataServer
	QueryServer   query_v1.QueryServer
	RoutingServer routing_v1.RoutingServer
}

type System struct {
	logger *zap.Logger

	kvServer    *servers.KvServer
	mgmtServer  *servers.MgmtServer
	queryServer *servers.QueryServer
}

func NewSystem(opts *SystemOptions) (*System, error) {
	mgmtServer, err := servers.NewMgmtServer(&servers.MgmtServerOptions{
		Logger:        opts.Logger,
		RoutingServer: opts.RoutingServer,
	})
	if err != nil {
		return nil, err
	}

	kvServer, err := servers.NewKvServer(&servers.KvServerOptions{
		Logger:        opts.Logger,
		DataServer:    opts.DataServer,
		RoutingServer: opts.RoutingServer,
	})
	if err != nil {
		return nil, err
	}

	queryServer, err := servers.NewQueryServer(&servers.QueryServerOptions{
		Logger:      opts.Logger,
		QueryServer: opts.QueryServer,
	})
	if err != nil {
		return nil, err
	}

	s := &System{
		logger:      opts.Logger,
		mgmtServer:  mgmtServer,
		kvServer:    kvServer,
		queryServer: queryServer,
	}

	return s, nil
}

func (s *System) Serve(ctx context.Context, l *Listeners) error {
	var wg sync.WaitGroup

	if l.mgmtListener != nil {
		wg.Add(1)
		go func() {
			err := s.mgmtServer.Serve(l.mgmtListener)
			if err != nil {
				s.logger.Warn("mgmt server serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	if l.kvListener != nil {
		wg.Add(1)
		go func() {
			err := s.kvServer.Serve(l.kvListener)
			if err != nil {
				s.logger.Warn("kv server serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	if l.queryListener != nil {
		wg.Add(1)
		go func() {
			err := s.queryServer.Serve(l.queryListener)
			if err != nil {
				s.logger.Warn("query server serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	if l.mgmtTLSListener != nil {
		wg.Add(1)
		go func() {
			err := s.mgmtServer.Serve(l.mgmtTLSListener)
			if err != nil {
				s.logger.Warn("mgmt server tls serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	if l.kvTLSListener != nil {
		wg.Add(1)
		go func() {
			err := s.kvServer.Serve(l.kvTLSListener)
			if err != nil {
				s.logger.Warn("kv server tls serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	if l.queryTLSListener != nil {
		wg.Add(1)
		go func() {
			err := s.queryServer.Serve(l.queryTLSListener)
			if err != nil {
				s.logger.Warn("query server tls serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}
