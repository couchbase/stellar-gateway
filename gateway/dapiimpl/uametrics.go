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

	"github.com/couchbase/stellar-gateway/utils/cbclientnames"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("github.com/couchbase/stellar-gateway/gateway/dapiimpl")
)

func NewUserAgentMetricsHandler() func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	clientNames, _ := meter.Int64Counter("dataapi_client_request_count")

	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			userAgent := r.Header.Get("user-agent")
			clientName := cbclientnames.FromUserAgent(userAgent)

			if clientName != "" {
				clientNames.Add(ctx, 1,
					metric.WithAttributes(attribute.String("client_name", clientName)))
			}

			return f(ctx, w, r, request)
		}
	}
}
