package dapiimpl

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	CbClient      *gocbcorex.BucketsTrackingAgentManager
	Authenticator auth.Authenticator

	Debug bool
}

type Servers struct {
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
		DataApiV1Server: server_v1.NewDataApiServer(
			opts.Logger.Named("dapi-server"),
			v1ErrHandler,
			v1AuthHandler),
	}
}
