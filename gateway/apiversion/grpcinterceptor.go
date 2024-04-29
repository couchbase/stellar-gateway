package apiversion

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func GrpcUnaryInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		apiVersion, err := GetAPIVersion(ctx)
		if err != nil {
			log.Debug("failed to parse api version header", zap.Error(err))
			return nil, status.New(codes.InvalidArgument,
				"failed to parse api version header").Err()
		}

		if apiVersion > LatestApiVersion {
			return nil, status.New(codes.Unimplemented,
				"specified api version is not supported").Err()
		}

		return handler(ctx, req)
	}
}

func GrpcStreamInterceptor(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		apiVersion, err := GetAPIVersion(ctx)
		if err != nil {
			log.Debug("failed to parse api version header", zap.Error(err))
			return status.New(codes.InvalidArgument, "failed to parse api version header").Err()
		}

		if apiVersion > LatestApiVersion {
			return status.New(codes.Unimplemented, "specified api version is not supported").Err()
		}

		return handler(srv, ss)
	}
}
