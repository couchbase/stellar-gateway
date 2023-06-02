package interceptors

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type ConnectionLoggingInterceptor struct {
	logger *zap.Logger
}

func NewConnectionLoggingInterceptor(log *zap.Logger) *ConnectionLoggingInterceptor {
	return &ConnectionLoggingInterceptor{
		logger: log,
	}
}

func (cli *ConnectionLoggingInterceptor) UnaryConnectionCounterInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response interface{}, err error) {
	p, _ := peer.FromContext(ctx)
	md, _ := metadata.FromIncomingContext(ctx)

	cli.logger.Info("Connection Established", zap.String("ip", p.Addr.String()), zap.Strings("user-agent", md.Get("user-agent")))
	
	resp, err := handler(ctx, req)
	
	cli.logger.Info("Connection Closed", zap.String("ip", p.Addr.String()), zap.Strings("user-agent", md.Get("user-agent")))

	return resp, err
}
