package server_v1

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
	"go.uber.org/zap"
)

type AnalyticsServer struct {
	analytics_v1.UnimplementedAnalyticsServiceServer
	logger   *zap.Logger
	cbClient *gocbcorex.AgentManager
}

func NewAnalyticsServer(cbClient *gocbcorex.AgentManager, logger *zap.Logger) *AnalyticsServer {
	return &AnalyticsServer{
		cbClient: cbClient,
		logger:   logger,
	}
}
