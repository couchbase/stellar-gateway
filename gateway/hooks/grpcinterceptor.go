/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package hooks

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func makeGrpcUnaryInterceptor(manager *HooksManager, log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		hooksIDs := metadata.ValueFromIncomingContext(ctx, "X-Hooks-ID")
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

		log.Info("calling registered hooks context", zap.String("hooks-id", hooksID), zap.Any("info", info), zap.Any("req", req))
		return hooksContext.HandleUnaryCall(ctx, req, info, handler)
	}
}
