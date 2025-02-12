package dapiimpl

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/propagation"

	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"go.opentelemetry.io/otel"
)

func NewOtelTracingHandler() func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			tp := otel.GetTextMapPropagator()
			ctx = tp.Extract(ctx, propagation.HeaderCarrier(r.Header))

			tracer := otel.GetTracerProvider().Tracer("github.com/couchbase/stellar-gateway/gateway/dapiimpl")
			ctx, span := tracer.Start(ctx, operationID)
			defer span.End()

			return f(ctx, w, r, request)
		}
	}
}
