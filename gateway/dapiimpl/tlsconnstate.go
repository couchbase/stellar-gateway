package dapiimpl

import (
	"context"
	"net/http"

	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
)

func NewTlsConnStateHandler() func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			ctx = context.WithValue(ctx, server_v1.CtxKeyTlsConnState{}, r.TLS)
			return f(ctx, w, r, request)
		}
	}
}
