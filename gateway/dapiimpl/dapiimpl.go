package dapiimpl

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/proxy"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	CbClient      *gocbcorex.BucketsTrackingAgentManager
	Authenticator auth.Authenticator

	ProxyServices   []proxy.ServiceType
	ProxyBlockAdmin bool
	Debug           bool

	Username string
	Password string
}

type Servers struct {
	DataApiProxy    *proxy.DataApiProxy
	DataApiV1Server dataapiv1.StrictServerInterface
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
		DataApiProxy: proxy.NewDataApiProxy(
			opts.Logger.Named("dapi-proxy"),
			opts.CbClient,
			opts.ProxyServices,
			opts.ProxyBlockAdmin,
			opts.Debug,
			v1AuthHandler,
			opts.Username,
			opts.Password,
		),
		DataApiV1Server: server_v1.NewDataApiServer(
			opts.Logger.Named("dapi-serverv1"),
			v1ErrHandler,
			v1AuthHandler),
	}
}
