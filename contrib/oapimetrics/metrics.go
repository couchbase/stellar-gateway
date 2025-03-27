/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package oapimetrics

import (
	"context"
	"net/http"
	"time"

	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var (
	meter = otel.Meter("github.com/couchbase/stellar-gateway/contrib/oapimetrics")
)

type statusCodeResponseWriter struct {
	BaseResponseWriter http.ResponseWriter
	StatusCode         int
}

var _ http.ResponseWriter = &statusCodeResponseWriter{}

func (w *statusCodeResponseWriter) Header() http.Header {
	return w.BaseResponseWriter.Header()
}

func (w *statusCodeResponseWriter) Write(b []byte) (int, error) {
	return w.BaseResponseWriter.Write(b)
}

func (w *statusCodeResponseWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.BaseResponseWriter.WriteHeader(statusCode)
}

func NewStatsHandler(logger *zap.Logger) func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	numRequests, err := meter.Int64Counter("oapi_server_requests")
	if err != nil {
		logger.Warn("failed to initialize request counter", zap.Error(err))
	}

	durationMillis, err := meter.Int64Histogram("oapi_server_duration_milliseconds")
	if err != nil {
		logger.Warn("failed to initialize duration histogram", zap.Error(err))
	}

	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			statusW := &statusCodeResponseWriter{
				BaseResponseWriter: w,
				StatusCode:         0,
			}

			stime := time.Now()

			resp, err := f(ctx, statusW, r, request)

			statusCode := statusW.StatusCode
			if statusCode == 0 {
				if err != nil {
					statusCode = http.StatusInternalServerError
				} else {
					statusCode = http.StatusOK
				}
			}

			etime := time.Now()
			dtime := etime.Sub(stime)
			dtimeMillis := dtime.Milliseconds()

			durationMillis.Record(ctx, dtimeMillis, metric.WithAttributes(
				attribute.String("operation_id", operationID),
				attribute.Int("http_status_code", statusCode),
			))

			numRequests.Add(ctx, 1, metric.WithAttributes(
				attribute.String("operation_id", operationID),
				attribute.Int("http_status_code", statusCode),
			))

			return resp, err
		}
	}
}
