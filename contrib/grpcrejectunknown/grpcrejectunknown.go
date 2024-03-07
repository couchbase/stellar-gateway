package grpcrejectunknown

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func MakeGrpcUnaryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		reqMsg := req.(proto.Message)

		unkFields := reqMsg.ProtoReflect().GetUnknown()
		if len(unkFields) > 0 {
			logger.Debug("rejecting request due to unexpected fields present in request",
				zap.ByteString("fields", unkFields))

			return nil, status.New(codes.Unimplemented, "An unimplemented field was encountered.").Err()
		}

		return handler(ctx, req)
	}
}
