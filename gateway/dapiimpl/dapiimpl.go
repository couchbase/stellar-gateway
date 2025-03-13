/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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

	ProxyServices []proxy.ServiceType
	Debug         bool
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
			opts.Debug),
		DataApiV1Server: server_v1.NewDataApiServer(
			opts.Logger.Named("dapi-serverv1"),
			v1ErrHandler,
			v1AuthHandler),
	}
}
