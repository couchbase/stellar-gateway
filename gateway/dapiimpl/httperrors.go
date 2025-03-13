/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package dapiimpl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/couchbase/stellar-gateway/gateway/dapiimpl/server_v1"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"go.uber.org/zap"
)

func NewErrorHandler(logger *zap.Logger) func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
	return func(f nethttp.StrictHTTPHandlerFunc, operationID string) nethttp.StrictHTTPHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request interface{}) (response interface{}, err error) {
			resp, err := f(ctx, w, r, request)
			if err != nil {
				var errSt *server_v1.StatusError
				if errors.As(err, &errSt) {
					errBytes, _ := json.Marshal(errSt.S)
					w.WriteHeader(errSt.S.StatusCode)
					_, _ = w.Write(errBytes)
					return nil, nil
				}
			}

			return resp, err
		}
	}
}
