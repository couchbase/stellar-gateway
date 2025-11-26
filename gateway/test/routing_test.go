package test

import (
	"context"
	"strings"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/goprotostellar/genproto/routing_v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestWatchRouting() {
	client := routing_v2.NewRoutingServiceClient(s.gatewayConn)

	s.Run("Success", func() {
		watchClient, err := client.WatchRouting(
			context.Background(),
			&routing_v2.WatchRoutingRequest{BucketName: &s.bucketName},
			grpc.PerRPCCredentials(s.basicRpcCreds),
		)
		require.NoError(s.T(), err)

		resp, err := watchClient.Recv()
		require.NoError(s.T(), err)

		bucketDef, err := s.testClusterInfo.AdminClient.GetBucket(
			context.Background(),
			&cbmgmtx.GetBucketOptions{BucketName: s.bucketName})
		require.NoError(s.T(), err)

		numNodes := len(bucketDef.RawConfig.Nodes)
		numVBuckets := len(bucketDef.RawConfig.VBucketServerMap.VBucketMap)

		require.Equal(s.T(), uint32(numVBuckets), resp.VbucketDataRouting.NumVbuckets)
		require.GreaterOrEqual(s.T(), len(resp.VbucketDataRouting.LocalVbuckets), numVBuckets/(numNodes+1))
		require.LessOrEqual(s.T(), len(resp.VbucketDataRouting.LocalVbuckets), numVBuckets/(numNodes-1))

		// The test gateway treats the node it was boostrapped against as
		// local so we check that all the vbIDs in the response local vBuckets
		// belong to our bootstrap node.
		for _, vbID := range resp.VbucketDataRouting.LocalVbuckets {
			srvIdx := bucketDef.RawConfig.VBucketServerMap.VBucketMap[vbID][0]
			vBucketHost := bucketDef.RawConfig.VBucketServerMap.ServerList[srvIdx]
			require.True(s.T(), strings.Contains(vBucketHost, s.testClusterInfo.ConnStr))
		}
	})

	s.Run("BucketNotFound", func() {
		bName := "missing-bucket"
		sClient, err := client.WatchRouting(
			context.Background(),
			&routing_v2.WatchRoutingRequest{BucketName: &bName},
			grpc.PerRPCCredentials(s.basicRpcCreds),
		)
		require.NoError(s.T(), err)

		_, err = sClient.Recv()
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("ServerRouting", func() {
		sClient, err := client.WatchRouting(
			context.Background(),
			&routing_v2.WatchRoutingRequest{},
			grpc.PerRPCCredentials(s.basicRpcCreds),
		)
		require.NoError(s.T(), err)

		_, err = sClient.Recv()
		// ING-1357 (support non-kv optimised routing)
		assertRpcStatus(s.T(), err, codes.Unimplemented)
	})
}
