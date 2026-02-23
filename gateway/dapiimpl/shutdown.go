package dapiimpl

import (
	"context"
	"net/http"

	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
)

func NewShutdownHandler(isShuttingDown func() bool) func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			if isShuttingDown() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return nil, nil
			}
			return f(ctx, w, r, request)
		}
	}
}
