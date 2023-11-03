package test

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestBucketManagement() {
	if !s.SupportsFeature(TestFeatureBucketManagement) {
		s.T().Skip()
	}

	adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)

	s.Run("CreateDefaults", func() {
		name := uuid.NewString()[:6]
		resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
			BucketName: name,
			BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		listResp, err := adminClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{},
			grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), listResp, err)

		var found *admin_bucket_v1.ListBucketsResponse_Bucket
		for _, b := range listResp.Buckets {
			if b.BucketName == name {
				found = b
				break
			}
		}
		if assert.NotNil(s.T(), found, "Did not find bucket on cluster") {
			assert.Equal(s.T(), admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE, found.BucketType)
			assert.Equal(s.T(), uint64(100), found.RamQuotaMb)
			assert.True(s.T(), found.ReplicaIndexes)
		}

		deleteResp, err := adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: name,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("CreateReplicaIndexDisabled", func() {
		name := uuid.NewString()[:6]
		replicaIndexes := false
		resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
			BucketName:     name,
			BucketType:     admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
			ReplicaIndexes: &replicaIndexes,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		listResp, err := adminClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{},
			grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		var found *admin_bucket_v1.ListBucketsResponse_Bucket
		for _, b := range listResp.Buckets {
			if b.BucketName == name {
				found = b
				break
			}
		}
		if assert.NotNil(s.T(), found, "Did not find bucket on cluster") {
			assert.False(s.T(), found.ReplicaIndexes)
		}

		deleteResp, err := adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: name,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("CreateEphemeralDefaults", func() {
		name := uuid.NewString()[:6]
		resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
			BucketName: name,
			BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		listResp, err := adminClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{},
			grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), listResp, err)

		var found *admin_bucket_v1.ListBucketsResponse_Bucket
		for _, b := range listResp.Buckets {
			if b.BucketName == name {
				found = b
				break
			}
		}
		if assert.NotNil(s.T(), found, "Did not find bucket on cluster") {
			assert.Equal(s.T(), admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL, found.BucketType)
			assert.Equal(s.T(), uint64(100), found.RamQuotaMb)
			assert.False(s.T(), found.ReplicaIndexes)
		}

		deleteResp, err := adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: name,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("CreateWithLessThan100MiBRAM", func() {
		ramQuota := uint64(99)
		name := uuid.NewString()[:6]
		_, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
			BucketName: name,
			BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
			RamQuotaMb: &ramQuota,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})
}
