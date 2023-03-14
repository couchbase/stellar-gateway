package system

import (
	"context"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// health server, it piggy backs on data since its publically exposed.
type HealthV1Server struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (s HealthV1Server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}
func (s HealthV1Server) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	return server.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING})
}
