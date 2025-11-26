package server_v1

import (
	"context"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/goprotostellar/genproto/routing_v2"
	"go.uber.org/zap"
)

type RoutingServer struct {
	routing_v2.UnimplementedRoutingServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler

	bootstrapNodeIsLocal bool

	// The hostname of the node that CNG was bootstrapped agaisnt. We can't just
	// use thisNode because that just depends on the node we happen to get the
	// bucket config from
	bootstrapHost string
}

func NewRoutingServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
	bootstrapNodeIsLocal bool,
	bootsrapHost string,
) *RoutingServer {
	return &RoutingServer{
		logger:               logger,
		errorHandler:         errorHandler,
		authHandler:          authHandler,
		bootstrapNodeIsLocal: bootstrapNodeIsLocal,
		bootstrapHost:        bootsrapHost,
	}
}

func (s *RoutingServer) WatchRouting(
	in *routing_v2.WatchRoutingRequest,
	out routing_v2.RoutingService_WatchRoutingServer,
) error {
	var bucketName string
	if in.BucketName == nil {
		// ING-1357 (support non-kv optimised routing)
		return s.errorHandler.NewServerRoutingUnimplementedError().Err()
	} else {
		bucketName = *in.BucketName
	}

	bucketAgent, _, errSt := s.authHandler.GetMemdOboAgent(out.Context(), bucketName)
	if errSt != nil {
		return errSt.Err()
	}

	ticker := time.NewTicker(5 * time.Second)
	var prevVBucketMap cbconfig.VBucketServerMapJson
	for {
		bucket, err := bucketAgent.GetBucket(context.Background(), &cbmgmtx.GetBucketOptions{BucketName: bucketName})
		if err != nil {
			return err
		}

		if !prevVBucketMap.Equal(*bucket.RawConfig.VBucketServerMap) {
			prevVBucketMap = *bucket.RawConfig.VBucketServerMap

			var localVBuckets []uint32
			for i, addr := range bucket.RawConfig.VBucketServerMap.ServerList {
				if strings.Contains(addr, s.bootstrapHost) && s.bootstrapNodeIsLocal {
					localVBuckets = vBucketIdsForServer(i, bucket.RawConfig.VBucketServerMap.VBucketMap)
				}
			}

			resp := &routing_v2.WatchRoutingResponse{
				VbucketDataRouting: &routing_v2.VbucketRouting{
					NumVbuckets:   uint32(len(bucket.RawConfig.VBucketServerMap.VBucketMap)),
					LocalVbuckets: localVBuckets,
				},
			}

			err = out.Send(resp)
			if err != nil {
				return err
			}
		}

		select {
		case <-out.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func vBucketIdsForServer(serverIndex int, vbMap [][]int) []uint32 {
	var vbIds []uint32
	for i, vBucket := range vbMap {
		if vBucket[0] == serverIndex {
			vbIds = append(vbIds, uint32(i))
		}
	}
	return vbIds
}
