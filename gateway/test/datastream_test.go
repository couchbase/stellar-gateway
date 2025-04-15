package test

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/datastream_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func (s *GatewayOpsTestSuite) TestDsStreamCollection() {
	if !s.SupportsFeature(TestFeatureDatastream) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
	dsClient := datastream_v1.NewDatastreamServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		resp, err := dsClient.StreamCollection(context.Background(), &datastream_v1.StreamCollectionRequest{
			BucketName:     "default",
			ScopeName:      "_default",
			CollectionName: "_default",
			Filter:         ptr.To("x = 1"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		require.NoError(s.T(), err)

		go func() {
			time.Sleep(2 * time.Second)

			_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     "default",
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            "key1",
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: []byte(`{"name":"value1", "x": 2}`),
				},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			require.NoError(s.T(), err)

			_, err = kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     "default",
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            "key1",
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: []byte(`{"name":"value1", "x": 1}`),
				},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			require.NoError(s.T(), err)
		}()

		for {
			itemResp, err := resp.Recv()
			if err != nil && errors.Is(err, io.EOF) {
				break
			}
			require.NoError(s.T(), err)

			log.Printf("RECEIVED ITEM: %v", itemResp)
		}
	})
}
