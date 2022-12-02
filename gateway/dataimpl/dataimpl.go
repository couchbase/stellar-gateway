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
		KvV1Server:              server_v1.NewKvServer(opts.CbClient),
		QueryV1Server:           server_v1.NewQueryServer(opts.CbClient),
		SearchV1Server:          server_v1.NewSearchServer(opts.CbClient),
		AnalyticsV1Server:       server_v1.NewAnalyticsServer(opts.CbClient),
		AdminBucketV1Server:     server_v1.NewBucketAdminServer(opts.CbClient),
		AdminCollectionV1Server: server_v1.NewCollectionAdminServer(opts.CbClient),
		TransactionsV1Server:    server_v1.NewTransactionsServer(opts.CbClient),
	}
}
