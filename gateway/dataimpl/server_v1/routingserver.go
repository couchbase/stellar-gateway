package server_v1

import (
	"context"
	"errors"
	"fmt"
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

	localhostConnstr bool
	bootstrapHost    string
}

func NewRoutingServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
	localhostConnstr bool,
	bootsrapHost string,
) *RoutingServer {
	return &RoutingServer{
		logger:           logger,
		errorHandler:     errorHandler,
		authHandler:      authHandler,
		localhostConnstr: localhostConnstr,
		bootstrapHost:    bootsrapHost,
	}
}

func (s *RoutingServer) WatchRouting(
	in *routing_v2.WatchRoutingRequest,
	out routing_v2.RoutingService_WatchRoutingServer,
) error {
	// var bucketName string
	// if in.BucketName == nil {
	// 	bucketName = ""
	// } else {
	// 	bucketName = *in.BucketName
	// }

	fmt.Println("JW HERE")
	bucketAgent, _, errSt := s.authHandler.GetMemdOboAgent(out.Context(), *in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	fmt.Println("JW HERE 1")

	// Get the vBucketMap
	// Get the node that CNG was bootstrapped against
	// Get the vbuckets local to that node
	// If the node is on localhost then these are local vBuckets
	// Else we ignore (in later work we will use this list of vBuckets)

	ticker := time.NewTicker(5 * time.Second)
	var prevVBucketMap cbconfig.VBucketServerMapJson
	for {
		bucket, err := bucketAgent.GetBucket(context.Background(), &cbmgmtx.GetBucketOptions{BucketName: *in.BucketName})
		if err != nil {
			return err
		}

		if !prevVBucketMap.Equal(*bucket.RawConfig.VBucketServerMap) {
			prevVBucketMap = *bucket.RawConfig.VBucketServerMap

			fmt.Println("JW NODES")
			fmt.Printf("%+v", bucket.RawConfig.Nodes)
			fmt.Println("JW SRVS")
			fmt.Printf("%+v", bucket.RawConfig.VBucketServerMap.ServerList)
			fmt.Println("JW VBUCKETS")
			fmt.Println(bucket.RawConfig.VBucketServerMap.VBucketMap)

			fmt.Println("JW IS LOCAL")
			fmt.Println(s.localhostConnstr)

			// Find the node that CNG was boostrapped against
			var srvIndex *int
			for i, addr := range bucket.RawConfig.VBucketServerMap.ServerList {
				if strings.Contains(addr, s.bootstrapHost) {
					srvIndex = &i
				}
			}

			if srvIndex == nil {
				return errors.New("could not match bootstrap node to bucket config server")
			}

			vBuckets := vBucketIdsForServer(*srvIndex, bucket.RawConfig.VBucketServerMap.VBucketMap)

			var localVBuckets []uint32
			var groupVBuckets []uint32
			if s.localhostConnstr {
				localVBuckets = vBuckets
			} else {
				// TODO - ING-1348 (determine group vBuckets in watch routing)
			}

			resp := &routing_v2.WatchRoutingResponse{
				VbucketDataRouting: &routing_v2.VbucketRouting{
					NumVbuckets:   uint32(len(localVBuckets) + len(groupVBuckets)),
					LocalVbuckets: localVBuckets,
					GroupVbuckets: groupVBuckets,
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
