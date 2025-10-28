package server_v1

import (
	"context"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/contrib/goprotostellar/genproto/routing_v1"
	"go.uber.org/zap"
)

type RoutingServer struct {
	routing_v1.UnimplementedRoutingServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
	cbClient     *gocbcorex.BucketsTrackingAgentManager
}

func NewRoutingServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
	cbClient *gocbcorex.BucketsTrackingAgentManager,
) *RoutingServer {
	return &RoutingServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
		cbClient:     cbClient,
	}
}

func (s *RoutingServer) WatchRouting(
	in *routing_v1.WatchRoutingRequest,
	out routing_v1.RoutingService_WatchRoutingServer,
) error {
	bucketAgent, _, errSt := s.authHandler.GetMemdOboAgent(out.Context(), *in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	ticker := time.NewTicker(5 * time.Second)
	var prevVBucketMap cbconfig.VBucketServerMapJson
	for {
		bucket, err := bucketAgent.GetBucket(context.Background(), &cbmgmtx.GetBucketOptions{BucketName: *in.BucketName})
		if err != nil {
			return err
		}

		if !prevVBucketMap.Equal(*bucket.RawConfig.VBucketServerMap) {
			prevVBucketMap = *bucket.RawConfig.VBucketServerMap

			var resp routing_v1.WatchRoutingResponse
			var dataRoutingEps []*routing_v1.DataRoutingEndpoint
			for i, addr := range bucket.RawConfig.VBucketServerMap.ServerList {
				resp.Endpoints = append(resp.Endpoints, &routing_v1.RoutingEndpoint{
					// TO DO -figure out what this ID is supposed to be
					Id: "",
					// TODO - get the server group from somewhere
					ServerGroup: "",
					Address:     addr,
				})

				localVBuckets := localVBuckets(i, bucket.RawConfig.VBucketServerMap.VBucketMap)

				dataRoutingEps = append(dataRoutingEps, &routing_v1.DataRoutingEndpoint{
					EndpointIdx:   uint32(i),
					LocalVbuckets: localVBuckets,
					// TODO - implement funcitonality to get group vBuckets
					GroupVbuckets: []uint32{},
				})
			}
			resp.DataRouting = &routing_v1.WatchRoutingResponse_VbucketDataRouting{
				VbucketDataRouting: &routing_v1.VbucketDataRoutingStrategy{
					Endpoints: dataRoutingEps,
				},
			}

			err = out.Send(&resp)
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

// At the moment this will just return the vBuckets that are local for the master
// copy, need to consider what to do about other replicas
func localVBuckets(serverIndex int, vbMap [][]int) []uint32 {
	var vbIds []uint32
	for i, vBucket := range vbMap {
		if vBucket[0] == serverIndex {
			vbIds = append(vbIds, uint32(i))
		}
	}
	return vbIds
}
