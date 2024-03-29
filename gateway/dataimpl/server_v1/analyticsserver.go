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
