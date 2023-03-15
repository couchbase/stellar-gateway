package sdimpl

import (
	"github.com/couchbase/stellar-gateway/gateway/sdimpl/server_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	TopologyProvider topology.Provider
}

type Servers struct {
	RoutingV1Server *server_v1.RoutingServer
}

func New(opts *NewOptions) *Servers {
	return &Servers{
		RoutingV1Server: server_v1.NewRoutingServer(opts.TopologyProvider, opts.Logger.Named("routing-server")),
	}
}
