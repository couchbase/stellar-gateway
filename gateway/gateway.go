package gateway

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/common/topology"
	"github.com/couchbase/stellar-nebula/gateway/server_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	admin_bucket_v1 "github.com/couchbase/stellar-nebula/genproto/admin/bucket/v1"
	analytics_v1 "github.com/couchbase/stellar-nebula/genproto/analytics/v1"
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	query_v1 "github.com/couchbase/stellar-nebula/genproto/query/v1"
	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
	search_v1 "github.com/couchbase/stellar-nebula/genproto/search/v1"
	transactions_v1 "github.com/couchbase/stellar-nebula/genproto/transactions/v1"
	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
)

// TODO(brett19): Implement the gateway system as its own component

type GatewayOptions struct {
	Logger *zap.Logger

	BindAddress string
	BindPort    int

	TopologyProvider topology.Provider
	CbClient         *gocb.Cluster
}

type Gateway struct {
	logger           *zap.Logger
	bindAddress      string
	bindPort         int
	topologyProvider topology.Provider
	cbClient         *gocb.Cluster

	grpcListener net.Listener
	grpcServer   *grpc.Server

	// This are intentionally public to allow external use
	CouchbaseV1Server    *server_v1.CouchbaseServer
	RoutingV1Server      *server_v1.RoutingServer
	DataV1Server         *server_v1.DataServer
	QueryV1Server        *server_v1.QueryServer
	SearchV1Server       *server_v1.SearchServer
	AnalyticsV1Server    *server_v1.AnalyticsServer
	AdminBucketV1Server  *server_v1.BucketAdminServer
	TransactionsV1Server *server_v1.TransactionsServer
}

func NewGateway(opts *GatewayOptions) (*Gateway, error) {
	g := &Gateway{
		logger:           opts.Logger,
		bindAddress:      opts.BindAddress,
		bindPort:         opts.BindPort,
		topologyProvider: opts.TopologyProvider,
		cbClient:         opts.CbClient,
	}

	err := g.init()
	if err != nil {
		return nil, err
	}

	return g, nil
}

func (g *Gateway) init() error {
	g.CouchbaseV1Server = server_v1.NewCouchbaseServer()
	g.RoutingV1Server = server_v1.NewRoutingServer(g.topologyProvider)
	g.DataV1Server = server_v1.NewDataServer(g.cbClient)
	g.QueryV1Server = server_v1.NewQueryServer(g.cbClient)
	g.SearchV1Server = server_v1.NewSearchServer(g.cbClient)
	g.AnalyticsV1Server = server_v1.NewAnalyticsServer(g.cbClient)
	g.AdminBucketV1Server = server_v1.NewBucketAdminServer(g.cbClient)
	g.TransactionsV1Server = server_v1.NewTransactionsServer(g.cbClient)

	s := grpc.NewServer()
	couchbase_v1.RegisterCouchbaseServer(s, g.CouchbaseV1Server)
	routing_v1.RegisterRoutingServer(s, g.RoutingV1Server)
	data_v1.RegisterDataServer(s, g.DataV1Server)
	query_v1.RegisterQueryServer(s, g.QueryV1Server)
	search_v1.RegisterSearchServer(s, g.SearchV1Server)
	analytics_v1.RegisterAnalyticsServer(s, g.AnalyticsV1Server)
	admin_bucket_v1.RegisterBucketAdminServer(s, g.AdminBucketV1Server)
	transactions_v1.RegisterTransactionsServer(s, g.TransactionsV1Server)
	g.grpcServer = s

	// TODO(brett19): Consider whether creating the listener here is a good idea...
	// Its possible that it might take a while to initialize everything before we can
	// start actually accepting clients from these listeners.  On the flip-side, we need
	// the listeners started early so we know what addr/port we are using...  Might need
	// to split this into 3 calls.  init(), Prepare(), Run() to get that behaviour.

	log.Printf("initializing grpc listener")
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.bindAddress, g.bindPort))
	if err != nil {
		return err
	}
	log.Printf("grpc listener is listening at %v", lis.Addr())
	g.grpcListener = lis

	return nil
}

func (g *Gateway) Run(ctx context.Context) error {
	err := g.grpcServer.Serve(g.grpcListener)
	if err != nil {
		log.Printf("failed to serve grpc: %v", err)
		return err
	}

	return nil
}
