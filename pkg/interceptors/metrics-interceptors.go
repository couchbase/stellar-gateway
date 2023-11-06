package interceptors

import (
	"context"

	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"google.golang.org/grpc"
)

type MetricsInterceptor struct {
	metrics *metrics.SnMetrics
}

func NewMetricsInterceptor(metrics *metrics.SnMetrics) *MetricsInterceptor {
	return &MetricsInterceptor{
		metrics: metrics,
	}
}

func (mi *MetricsInterceptor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
		mi.metrics.NewConnections.Add(1)
		mi.metrics.ActiveConnections.Inc()

		resp, err := handler(ctx, req)

		mi.metrics.ActiveConnections.Dec()

		return resp, err
	}
}

func (mi *MetricsInterceptor) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		mi.metrics.NewConnections.Add(1)
		mi.metrics.ActiveConnections.Inc()

		err := handler(srv, ss)

		mi.metrics.ActiveConnections.Dec()

		return err
	}
}
