/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
