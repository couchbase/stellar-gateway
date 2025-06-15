/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
	"go.uber.org/zap"
)

type AnalyticsServer struct {
	analytics_v1.UnimplementedAnalyticsServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewAnalyticsServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *AnalyticsServer {
	return &AnalyticsServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}
