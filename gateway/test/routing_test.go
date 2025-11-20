package test

import (
	"context"
	"strings"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/goprotostellar/genproto/routing_v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func (s *GatewayOpsTestSuite) TestWatchRouting() {
	client := routing_v2.NewRoutingServiceClient(s.gatewayConn)

	// s.Run("BucketNotFound", func() {
	// 	bName := "missing-bucket"
	// 	rClient, err := client.WatchRouting(
	// 		context.Background(),
	// 		&routing_v1.WatchRoutingRequest{BucketName: &bName},
	// 		grpc.PerRPCCredentials(s.basicRpcCreds),
	// 	)
	// 	assertRpcStatus(s.T(), err, codes.NotFound)

	// 	time.Sleep(time.Second * 5)
	// })

	s.Run("LocalBuckets", func() {
		watchClient, err := client.WatchRouting(
			context.Background(),
			&routing_v2.WatchRoutingRequest{BucketName: &s.bucketName},
			grpc.PerRPCCredentials(s.basicRpcCreds),
		)
		require.NoError(s.T(), err)

		resp, err := watchClient.Recv()
		require.NoError(s.T(), err)

		// We test against a 3 node cluster, hence the 341
		require.GreaterOrEqual(s.T(), resp.VbucketDataRouting.NumVbuckets, uint32(341))

		bucketDef, err := s.testClusterInfo.AdminClient.GetBucket(
			context.Background(),
			&cbmgmtx.GetBucketOptions{BucketName: s.bucketName})
		require.NoError(s.T(), err)

		// The test gateway treats the node it was boostrapped against as localhost
		// so we check that all the vbIDs in the response local vBuckets belong
		// to our bootstrap node.
		for _, vbID := range resp.VbucketDataRouting.LocalVbuckets {
			srvIdx := bucketDef.RawConfig.VBucketServerMap.VBucketMap[vbID][0]
			vBucketHost := bucketDef.RawConfig.VBucketServerMap.ServerList[srvIdx]
			require.True(s.T(), strings.Contains(vBucketHost, s.testClusterInfo.ConnStr))
		}
	})

	// Furhter tests
	// Bucket name missing
	// Bucket not found
}
