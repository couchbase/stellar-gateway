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

func (mi *MetricsInterceptor) UnaryConnectionCounterInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	mi.metrics.NewConnections.Add(1)
	mi.metrics.ActiveConnections.Inc()

	resp, err := handler(ctx, req)

	mi.metrics.ActiveConnections.Dec()

	return resp, err
}
