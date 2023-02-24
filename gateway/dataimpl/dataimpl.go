package dataimpl

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl/server_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
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

func (s *Servers) UpdateClient(cbClient *gocb.Cluster) {
	s.AdminBucketV1Server.UpdateClient(cbClient)
	s.QueryV1Server.UpdateClient(cbClient)
	s.KvV1Server.UpdateClient(cbClient)
	s.SearchV1Server.UpdateClient(cbClient)
	s.AnalyticsV1Server.UpdateClient(cbClient)
	s.AdminCollectionV1Server.UpdateClient(cbClient)
	s.TransactionsV1Server.UpdateClient(cbClient)
}

func New(opts *NewOptions) *Servers {
	return &Servers{
		KvV1Server:              server_v1.NewKvServer(opts.CbClient, opts.Logger),
		QueryV1Server:           server_v1.NewQueryServer(opts.CbClient, opts.Logger),
		SearchV1Server:          server_v1.NewSearchServer(opts.CbClient, opts.Logger),
		AnalyticsV1Server:       server_v1.NewAnalyticsServer(opts.CbClient, opts.Logger),
		AdminBucketV1Server:     server_v1.NewBucketAdminServer(opts.CbClient, opts.Logger),
		AdminCollectionV1Server: server_v1.NewCollectionAdminServer(opts.CbClient, opts.Logger),
		TransactionsV1Server:    server_v1.NewTransactionsServer(opts.CbClient, opts.Logger),
	}
}
