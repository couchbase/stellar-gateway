package dataimpl

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl/server_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	TopologyProvider topology.Provider
	CbClient         *gocbcorex.AgentManager
	Authenticator    auth.Authenticator
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
	v1AuthHandler := &server_v1.AuthHandler{
		Logger:        opts.Logger.Named("auth"),
		Authenticator: opts.Authenticator,
		CbClient:      opts.CbClient,
	}

	return &Servers{
		KvV1Server: server_v1.NewKvServer(
			opts.Logger.Named("kv"),
			v1AuthHandler,
		),
		QueryV1Server: server_v1.NewQueryServer(
			opts.Logger.Named("query"),
			v1AuthHandler,
		),
		SearchV1Server: server_v1.NewSearchServer(
			opts.Logger.Named("search"),
			v1AuthHandler,
		),
		AnalyticsV1Server: server_v1.NewAnalyticsServer(
			opts.Logger.Named("analytics"),
			v1AuthHandler,
		),
		AdminBucketV1Server: server_v1.NewBucketAdminServer(
			opts.Logger.Named("adminbucket"),
			v1AuthHandler,
		),
		AdminCollectionV1Server: server_v1.NewCollectionAdminServer(
			opts.Logger.Named("admincollection"),
			v1AuthHandler,
		),
		TransactionsV1Server: server_v1.NewTransactionsServer(
			opts.Logger.Named("transactions"),
			v1AuthHandler,
		),
	}
}
