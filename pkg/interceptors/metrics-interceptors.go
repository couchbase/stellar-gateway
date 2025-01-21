package interceptors

import (
	"context"

	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/couchbase/stellar-gateway/utils/cbclientnames"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type MetricsInterceptor struct {
	metrics *metrics.SnMetrics
}

func NewMetricsInterceptor(metrics *metrics.SnMetrics) *MetricsInterceptor {
	return &MetricsInterceptor{
		metrics: metrics,
	}
}

func (mi *MetricsInterceptor) recordClient(ctx context.Context) {
	userAgents := metadata.ValueFromIncomingContext(ctx, "User-Agent")
	if len(userAgents) > 0 {
		userAgent := userAgents[len(userAgents)-1]
		clientName := cbclientnames.FromUserAgent(userAgent)

		if clientName != "" {
			mi.metrics.ClientNames.Add(ctx, 1,
				metric.WithAttributes(attribute.String("client_name", clientName)))
		}
	}
}

func (mi *MetricsInterceptor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
		switch otel.GetMeterProvider().(type) {
		case noop.MeterProvider:
			return handler(ctx, req)
		}

		mi.recordClient(ctx)

		mi.metrics.NewConnections.Add(ctx, 1)
		mi.metrics.ActiveConnections.Add(ctx, 1)

		resp, err := handler(ctx, req)

		mi.metrics.ActiveConnections.Add(ctx, -1)

		return resp, err
	}
}

func (mi *MetricsInterceptor) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		switch otel.GetMeterProvider().(type) {
		case noop.MeterProvider:
			return handler(srv, ss)
		}

		mi.recordClient(ss.Context())

		mi.metrics.NewConnections.Add(ss.Context(), 1)
		mi.metrics.ActiveConnections.Add(ss.Context(), 1)

		err := handler(srv, ss)

		mi.metrics.ActiveConnections.Add(ss.Context(), -1)

		return err
	}
}
