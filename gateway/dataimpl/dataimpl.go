package dataimpl

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl/server_v1"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	CbClient      *gocbcorex.BucketsTrackingAgentManager
	Authenticator auth.Authenticator

	Debug bool
}

type Servers struct {
	KvV1Server               *server_v1.KvServer
	QueryV1Server            *server_v1.QueryServer
	SearchV1Server           *server_v1.SearchServer
	AnalyticsV1Server        *server_v1.AnalyticsServer
	AdminBucketV1Server      *server_v1.BucketAdminServer
	AdminCollectionV1Server  *server_v1.CollectionAdminServer
	AdminQueryIndexV1Server  *server_v1.QueryIndexAdminServer
	AdminSearchIndexV1Server *server_v1.SearchIndexAdminServer
	TransactionsV1Server     *server_v1.TransactionsServer
	RoutingServer            *server_v1.RoutingServer
}

func New(opts *NewOptions) *Servers {
	v1ErrHandler := &server_v1.ErrorHandler{
		Logger: opts.Logger.Named("errors"),
		Debug:  opts.Debug,
	}

	v1AuthHandler := &server_v1.AuthHandler{
		Logger:        opts.Logger.Named("auth"),
		ErrorHandler:  v1ErrHandler,
		Authenticator: opts.Authenticator,
		CbClient:      opts.CbClient,
	}

	return &Servers{
		KvV1Server: server_v1.NewKvServer(
			opts.Logger.Named("kv"),
			v1ErrHandler,
			v1AuthHandler,
		),
		QueryV1Server: server_v1.NewQueryServer(
			opts.Logger.Named("query"),
			v1ErrHandler,
			v1AuthHandler,
		),
		SearchV1Server: server_v1.NewSearchServer(
			opts.Logger.Named("search"),
			v1ErrHandler,
			v1AuthHandler,
		),
		AnalyticsV1Server: server_v1.NewAnalyticsServer(
			opts.Logger.Named("analytics"),
			v1ErrHandler,
			v1AuthHandler,
		),
		AdminBucketV1Server: server_v1.NewBucketAdminServer(
			opts.Logger.Named("adminbucket"),
			v1ErrHandler,
			v1AuthHandler,
		),
		AdminCollectionV1Server: server_v1.NewCollectionAdminServer(
			opts.Logger.Named("admincollection"),
			v1ErrHandler,
			v1AuthHandler,
		),
		AdminQueryIndexV1Server: server_v1.NewQueryIndexAdminServer(
			opts.Logger.Named("adminqueryindex"),
			v1ErrHandler,
			v1AuthHandler,
		),
		AdminSearchIndexV1Server: server_v1.NewSearchIndexAdminServer(
			opts.Logger.Named("adminsearchindex"),
			v1ErrHandler,
			v1AuthHandler,
		),
		TransactionsV1Server: server_v1.NewTransactionsServer(
			opts.Logger.Named("transactions"),
			v1ErrHandler,
			v1AuthHandler,
		),
		RoutingServer: server_v1.NewRoutingServer(
			opts.Logger.Named("routing"),
			v1ErrHandler,
			v1AuthHandler,
			opts.CbClient,
		),
	}
}
