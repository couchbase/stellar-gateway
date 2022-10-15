package psimpl

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/common/pstopology"
	"github.com/couchbase/stellar-nebula/genproto/admin_bucket_v1"
	"github.com/couchbase/stellar-nebula/genproto/analytics_v1"
	"github.com/couchbase/stellar-nebula/genproto/couchbase_v1"
	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/genproto/search_v1"
	"github.com/couchbase/stellar-nebula/genproto/transactions_v1"
	"github.com/couchbase/stellar-nebula/psimpl/server_v1"
	"go.uber.org/zap"
)

type GatewayOptions struct {
	Logger *zap.Logger

	TopologyProvider pstopology.Provider
	CbClient         *gocb.Cluster
}

type Gateway struct {
	logger           *zap.Logger
	topologyProvider pstopology.Provider
	cbClient         *gocb.Cluster

	// This are intentionally public to allow external use
	couchbaseV1Server    *server_v1.CouchbaseServer
	routingV1Server      *server_v1.RoutingServer
	dataV1Server         *server_v1.DataServer
	queryV1Server        *server_v1.QueryServer
	searchV1Server       *server_v1.SearchServer
	analyticsV1Server    *server_v1.AnalyticsServer
	adminBucketV1Server  *server_v1.BucketAdminServer
	transactionsV1Server *server_v1.TransactionsServer
}

func NewGateway(opts *GatewayOptions) (*Gateway, error) {
	g := &Gateway{
		logger:           opts.Logger,
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
	g.couchbaseV1Server = server_v1.NewCouchbaseServer()
	g.routingV1Server = server_v1.NewRoutingServer(g.topologyProvider)
	g.dataV1Server = server_v1.NewDataServer(g.cbClient)
	g.queryV1Server = server_v1.NewQueryServer(g.cbClient)
	g.searchV1Server = server_v1.NewSearchServer(g.cbClient)
	g.analyticsV1Server = server_v1.NewAnalyticsServer(g.cbClient)
	g.adminBucketV1Server = server_v1.NewBucketAdminServer(g.cbClient)
	g.transactionsV1Server = server_v1.NewTransactionsServer(g.cbClient)

	return nil
}

func (g *Gateway) CouchbaseV1() couchbase_v1.CouchbaseServer          { return g.couchbaseV1Server }
func (g *Gateway) RoutingV1() routing_v1.RoutingServer                { return g.routingV1Server }
func (g *Gateway) DataV1() data_v1.DataServer                         { return g.dataV1Server }
func (g *Gateway) QueryV1() query_v1.QueryServer                      { return g.queryV1Server }
func (g *Gateway) SearchV1() search_v1.SearchServer                   { return g.searchV1Server }
func (g *Gateway) AnalyticsV1() analytics_v1.AnalyticsServer          { return g.analyticsV1Server }
func (g *Gateway) AdminBucketV1() admin_bucket_v1.BucketAdminServer   { return g.adminBucketV1Server }
func (g *Gateway) TransactionsV1() transactions_v1.TransactionsServer { return g.transactionsV1Server }
