package system

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/health_v1"
)

// health server, it piggy backs on data since its publically exposed.
type HealthV1Server struct {
	health_v1.UnimplementedHealthServer
}

func (s HealthV1Server) Check(ctx context.Context, req *health_v1.HealthCheckRequest) (*health_v1.HealthCheckResponse, error) {
	return &health_v1.HealthCheckResponse{
		Status: health_v1.HealthCheckResponse_SERVING,
	}, nil
}
func (s HealthV1Server) Watch(req *health_v1.HealthCheckRequest, server health_v1.Health_WatchServer) error {
	return server.Send(&health_v1.HealthCheckResponse{Status: health_v1.HealthCheckResponse_SERVING})
}
