package dapiimpl

import (
	"context"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

func NewUserAgentTracingHandler() func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			tp := otel.GetTextMapPropagator()
			ctx = tp.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			var tracer trace.Tracer
			if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
				tracer = newTracer(span.TracerProvider())
			} else {
				tracer = newTracer(otel.GetTracerProvider())
			}

			ctx, span := tracer.Start(ctx, operationID)
			defer span.End()

			return f(ctx, w, r, request)
		}
	}
}

func newTracer(tp trace.TracerProvider) trace.Tracer {
	return tp.Tracer("github.com/couchbase/stellar-gateway/gateway/dapiimpl")
}
