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
		KvV1Server: server_v1.NewKvServer(
			opts.Logger.Named("kv"),
			opts.CbClient,
		),
		QueryV1Server: server_v1.NewQueryServer(
			opts.Logger.Named("query"),
			opts.CbClient,
		),
		SearchV1Server: server_v1.NewSearchServer(
			opts.Logger.Named("search"),
			opts.CbClient,
		),
		AnalyticsV1Server: server_v1.NewAnalyticsServer(
			opts.Logger.Named("analytics"),
			opts.CbClient,
		),
		AdminBucketV1Server: server_v1.NewBucketAdminServer(
			opts.Logger.Named("adminbucket"),
			opts.CbClient,
		),
		AdminCollectionV1Server: server_v1.NewCollectionAdminServer(
			opts.Logger.Named("admincollection"),
			opts.CbClient,
		),
		TransactionsV1Server: server_v1.NewTransactionsServer(
			opts.Logger.Named("transactions"),
			opts.CbClient,
		),
	}
}
