package legacyproxy

import (
	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/legacyproxy/servers"
	"go.uber.org/zap"
)

type ServicePorts struct {
	Mgmt      int
	Kv        int
	Query     int
	Search    int
	Analytics int
}

type SystemOptions struct {
	Logger       *zap.Logger
	BindAddress  string
	BindPorts    ServicePorts
	TLSBindPorts ServicePorts

	DataServer    data_v1.DataServer
	QueryServer   query_v1.QueryServer
	RoutingServer routing_v1.RoutingServer
}

type System struct {
	logger        *zap.Logger
	bindAddress   string
	bindPorts     ServicePorts
	tlsBindPorts  ServicePorts
	dataServer    data_v1.DataServer
	queryServer   query_v1.QueryServer
	routingServer routing_v1.RoutingServer

	kvServer   *servers.KvServer
	mgmtServer *servers.MgmtServer

	kvTlsServer   *servers.KvServer
	mgmtTlsServer *servers.MgmtServer
}

func NewSystem(opts *SystemOptions) (*System, error) {
	s := &System{
		logger:        opts.Logger,
		bindAddress:   opts.BindAddress,
		bindPorts:     opts.BindPorts,
		tlsBindPorts:  opts.TLSBindPorts,
		dataServer:    opts.DataServer,
		queryServer:   opts.QueryServer,
		routingServer: opts.RoutingServer,
	}

	err := s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *System) init() error {
	kvServer, err := servers.NewKvServer(&servers.KvServerOptions{
		Logger:        s.logger,
		BindAddress:   s.bindAddress,
		BindPort:      s.bindPorts.Kv,
		TlsConfig:     nil,
		DataServer:    s.dataServer,
		RoutingServer: s.routingServer,
	})
	if err != nil {
		return err
	}

	mgmtServer, err := servers.NewMgmtServer(&servers.MgmtServerOptions{
		Logger: s.logger,

		BindAddress: s.bindAddress,
		BindPort:    s.bindPorts.Mgmt,
		TlsConfig:   nil,

		RoutingServer: s.routingServer,
	})
	if err != nil {
		return err
	}

	s.kvServer = kvServer
	s.mgmtServer = mgmtServer

	s.kvTlsServer = nil
	s.mgmtTlsServer = nil

	return nil
}

func (s *System) Test() {

}
