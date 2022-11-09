package dataimpl

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/gateway/dataimpl/server_v1"
	"github.com/couchbase/stellar-nebula/gateway/topology"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	TopologyProvider topology.Provider
	CbClient         *gocb.Cluster
}

type Servers struct {
	DataV1Server         *server_v1.DataServer
	QueryV1Server        *server_v1.QueryServer
	SearchV1Server       *server_v1.SearchServer
	AnalyticsV1Server    *server_v1.AnalyticsServer
	AdminBucketV1Server  *server_v1.BucketAdminServer
	TransactionsV1Server *server_v1.TransactionsServer
}

func New(opts *NewOptions) *Servers {
	return &Servers{
		DataV1Server:         server_v1.NewDataServer(opts.CbClient),
		QueryV1Server:        server_v1.NewQueryServer(opts.CbClient),
		SearchV1Server:       server_v1.NewSearchServer(opts.CbClient),
		AnalyticsV1Server:    server_v1.NewAnalyticsServer(opts.CbClient),
		AdminBucketV1Server:  server_v1.NewBucketAdminServer(opts.CbClient),
		TransactionsV1Server: server_v1.NewTransactionsServer(opts.CbClient),
	}
}
