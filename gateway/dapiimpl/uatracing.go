package dapiimpl

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/propagation"

	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.opentelemetry.io/otel"
)

func NewOtelTracingHandler() dataapiv1.StrictMiddlewareFunc {
	return func(f dataapiv1.StrictHandlerFunc, operationID string) dataapiv1.StrictHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
			tp := otel.GetTextMapPropagator()
			ctx = tp.Extract(ctx, propagation.HeaderCarrier(r.Header))

			tracer := otel.GetTracerProvider().Tracer("github.com/couchbase/stellar-gateway/gateway/dapiimpl")
			ctx, span := tracer.Start(ctx, operationID)
			defer span.End()

			return f(ctx, w, r, request)
		}
	}
}
