package server_v1

import (
	"errors"
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
	mgmt         *cbmgmtx.Management

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
	mgmt *cbmgmtx.Management,
	bootstrapNodeIsLocal bool,
	bootsrapHost string,
) *RoutingServer {
	mgmt.UserAgent = "routing-server"

	return &RoutingServer{
		logger:               logger,
		errorHandler:         errorHandler,
		authHandler:          authHandler,
		mgmt:                 mgmt,
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
		return s.errorHandler.NewServerRoutingUnimplementedError(out.Context()).Err()
	} else {
		bucketName = *in.BucketName
	}

	_, oboInfo, errSt := s.authHandler.GetHttpOboAgent(out.Context(), nil)
	if errSt != nil {
		return errSt.Err()
	}

	ticker := time.NewTicker(5 * time.Second)
	var prevVBucketMap cbconfig.VBucketServerMapJson
	for {
		bucket, err := s.mgmt.GetTerseBucketConfig(out.Context(), &cbmgmtx.GetTerseBucketConfigOptions{
			BucketName: bucketName,
			OnBehalfOf: oboInfo,
		})

		if err != nil {
			if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
				return s.errorHandler.NewBucketMissingStatus(out.Context(), err, bucketName).Err()
			}

			return s.errorHandler.NewGenericStatus(out.Context(), err).Err()
		}

		if !prevVBucketMap.Equal(*bucket.VBucketServerMap) {
			prevVBucketMap = *bucket.VBucketServerMap

			var localVBuckets []uint32
			for i, node := range bucket.Nodes {
				if node.Hostname == s.bootstrapHost && s.bootstrapNodeIsLocal {
					localVBuckets = vBucketIdsForServer(i, bucket.VBucketServerMap.VBucketMap)
				}
			}

			resp := &routing_v2.WatchRoutingResponse{
				VbucketDataRouting: &routing_v2.VbucketRouting{
					NumVbuckets:   uint32(len(bucket.VBucketServerMap.VBucketMap)),
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
