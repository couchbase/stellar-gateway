package dataimpl

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl/server_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	TopologyProvider topology.Provider
	CbClient         *gocbcorex.AgentManager
}

type Servers struct {
	KvV1Server              *server_v1.KvServer
	QueryV1Server           *server_v1.QueryServer
	SearchV1Server          *server_v1.SearchServer
	AnalyticsV1Server       *server_v1.AnalyticsServer
	AdminBucketV1Server     *server_v1.BucketAdminServer
	AdminCollectionV1Server *server_v1.CollectionAdminServer
	TransactionsV1Server    *server_v1.TransactionsServer
}

func New(opts *NewOptions) *Servers {
	return &Servers{
		KvV1Server:              server_v1.NewKvServer(opts.CbClient, opts.Logger.Named("kv")),
		QueryV1Server:           server_v1.NewQueryServer(opts.CbClient, opts.Logger.Named("query")),
		SearchV1Server:          server_v1.NewSearchServer(opts.CbClient, opts.Logger.Named("search")),
		AnalyticsV1Server:       server_v1.NewAnalyticsServer(opts.CbClient, opts.Logger.Named("analytics")),
		AdminBucketV1Server:     server_v1.NewBucketAdminServer(opts.CbClient, opts.Logger.Named("adminbucket")),
		AdminCollectionV1Server: server_v1.NewCollectionAdminServer(opts.CbClient, opts.Logger.Named("admincollection")),
		TransactionsV1Server:    server_v1.NewTransactionsServer(opts.CbClient, opts.Logger.Named("transactions")),
	}
}
