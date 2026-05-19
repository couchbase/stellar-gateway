package dapiimpl

import (
	"context"
	"net/http"

	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
)

func NewTlsConnStateHandler() dataapiv1.StrictMiddlewareFunc {
	return func(f dataapiv1.StrictHandlerFunc, operationID string) dataapiv1.StrictHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
			ctx = context.WithValue(ctx, server_v1.CtxKeyTlsConnState{}, r.TLS)
			return f(ctx, w, r, request)
		}
	}
}
