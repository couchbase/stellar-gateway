package dapiimpl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
	"go.uber.org/zap"
)

func NewErrorHandler(logger *zap.Logger) dataapiv1.StrictMiddlewareFunc {
	return func(f dataapiv1.StrictHandlerFunc, operationID string) dataapiv1.StrictHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
			resp, err := f(ctx, w, r, request)
			if err != nil {
				var errSt *server_v1.StatusError
				if errors.As(err, &errSt) {
					errBytes, _ := json.Marshal(errSt.Data)
					w.WriteHeader(errSt.StatusCode)
					_, _ = w.Write(errBytes)
					return nil, nil
				}
			}

			return resp, err
		}
	}
}
