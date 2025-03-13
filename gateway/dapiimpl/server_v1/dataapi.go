/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"strconv"

	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.uber.org/zap"
)

type DataApiServer struct {
	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

var _ dataapiv1.StrictServerInterface = &DataApiServer{}

func NewDataApiServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *DataApiServer {
	return &DataApiServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *DataApiServer) parseKey(key string) ([]byte, *Status) {
	if len(key) > 250 || len(key) < 1 {
		return nil, s.errorHandler.NewInvalidKeyLengthStatus(key)
	}

	return []byte(key), nil
}

func (s *DataApiServer) parseCAS(etag *string) (uint64, *Status) {
	if etag != nil {
		casUint, err := strconv.ParseUint(*etag, 16, 64)
		if err != nil {
			return 0, s.errorHandler.NewInvalidEtagFormatStatus(*etag)
		}

		if casUint == 0 {
			return 0, s.errorHandler.NewZeroCasStatus()
		}

		return casUint, nil
	}

	return 0, nil
}
