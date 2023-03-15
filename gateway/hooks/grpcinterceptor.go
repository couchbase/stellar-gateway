package hooks

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func makeGrpcUnaryInterceptor(manager *HooksManager, log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if md == nil {
			// forward the underlying call
			return handler(ctx, req)
		}

		hooksIDs := md.Get("X-Hooks-ID")
		if hooksIDs == nil {
			// forward the underlying call
			return handler(ctx, req)
		}

		hooksID := hooksIDs[len(hooksIDs)-1]
		hooksContext := manager.GetHooksContext(hooksID)
		if hooksContext == nil {
			// forward the underlying call
			return handler(ctx, req)
		}

		log.Info("calling registered hooks context", zap.String("hooks-id", hooksID), zap.Any("info",info), zap.Any("req", req))
		return hooksContext.HandleUnaryCall(ctx, req, info, handler)
	}
}
