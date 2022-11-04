package hooks

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func makeGrpcInterceptor(manager *HooksManager) grpc.UnaryServerInterceptor {
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
		interceptor := manager.GetInterceptor(hooksID)
		if interceptor == nil {
			// forward the underlying call
			return handler(ctx, req)
		}

		log.Printf("calling registered interceptor: %s %+v %+v", hooksID, info, req)
		return interceptor.HandleUnaryCall(ctx, req, info, handler)
	}
}
