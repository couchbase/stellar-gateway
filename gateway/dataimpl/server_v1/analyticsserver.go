package server_v1

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
)

type AnalyticsServer struct {
	analytics_v1.UnimplementedAnalyticsServiceServer

	cbClient *gocbcorex.AgentManager
}

func NewAnalyticsServer(cbClient *gocbcorex.AgentManager) *AnalyticsServer {
	return &AnalyticsServer{
		cbClient: cbClient,
	}
}
