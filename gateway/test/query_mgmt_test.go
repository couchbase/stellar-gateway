package test

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func (s *GatewayOpsTestSuite) TestQueryManagement() {
	queryAdminClient := admin_query_v1.NewQueryAdminServiceClient(s.gatewayConn)

	findIndex := func(
		idxs []*admin_query_v1.GetAllIndexesResponse_Index,
		bucket, scope, collection string,
		name string,
	) *admin_query_v1.GetAllIndexesResponse_Index {
		for _, idx := range idxs {
			if idx.BucketName == bucket &&
				idx.ScopeName == scope &&
				idx.CollectionName == collection &&
				idx.Name == name {
				return idx
			}
		}
		return nil
	}

	s.Run("PrimaryIndex", func() {
		indexName := uuid.NewString()

		s.Run("Create", func() {
			resp, err := queryAdminClient.CreatePrimaryIndex(context.Background(), &admin_query_v1.CreatePrimaryIndexRequest{
				Name:           &indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})

		checkHasIndex := func(req *admin_query_v1.GetAllIndexesRequest) {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.NotNil(s.T(), foundIdx)
		}
		s.Run("ListAllIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{})
		})
		s.Run("ListBucketIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName: &s.bucketName,
			})
		})
		s.Run("ListScopeIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName: &s.bucketName,
				ScopeName:  &s.scopeName,
			})
		})
		s.Run("ListCollectionIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName:     &s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			})
		})

		checkNoHasIndex := func(req *admin_query_v1.GetAllIndexesRequest) {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.Nil(s.T(), foundIdx)
		}
		unmatchedName := "something-else"
		// BUG(ING-506): Can't test with invalid bucket names
		/*
			s.Run("NoListBucketIndexes", func() {
				checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
					BucketName: &unmatchedName,
				})
			})
		*/
		s.Run("NoListScopeIndexes", func() {
			checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName: &s.bucketName,
				ScopeName:  &unmatchedName,
			})
		})
		s.Run("NoListCollectionIndexes", func() {
			checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName:     &s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &unmatchedName,
			})
		})

		s.Run("Drop", func() {
			resp, err := queryAdminClient.DropPrimaryIndex(context.Background(), &admin_query_v1.DropPrimaryIndexRequest{
				Name:           &indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})
	})

	s.Run("SecondaryIndex", func() {
		indexName := uuid.NewString()

		s.Run("Create", func() {
			resp, err := queryAdminClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
				Name:           indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
				Fields:         []string{"test"},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})

		checkHasIndex := func(req *admin_query_v1.GetAllIndexesRequest) {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.NotNil(s.T(), foundIdx)
		}
		s.Run("ListAllIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{})
		})
		s.Run("ListBucketIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName: &s.bucketName,
			})
		})
		s.Run("ListScopeIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName: &s.bucketName,
				ScopeName:  &s.scopeName,
			})
		})
		s.Run("ListCollectionIndexes", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName:     &s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			})
		})

		checkNoHasIndex := func(req *admin_query_v1.GetAllIndexesRequest) {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.Nil(s.T(), foundIdx)
		}
		unmatchedName := "something-else"
		// BUG(ING-506): Can't test with invalid bucket names
		/*
			s.Run("NoListBucketIndexes", func() {
				checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
					BucketName: &unmatchedName,
				})
			})
		*/
		s.Run("NoListScopeIndexes", func() {
			checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName: &s.bucketName,
				ScopeName:  &unmatchedName,
			})
		})
		s.Run("NoListCollectionIndexes", func() {
			checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName:     &s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &unmatchedName,
			})
		})

		s.Run("Drop", func() {
			resp, err := queryAdminClient.DropIndex(context.Background(), &admin_query_v1.DropIndexRequest{
				Name:           indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})
	})

	s.Run("DeferredIndex", func() {
		indexName := uuid.NewString()

		s.Run("Create", func() {
			trueBool := true
			resp, err := queryAdminClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
				Name:           indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
				Fields:         []string{"test"},
				Deferred:       &trueBool,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})

		s.Run("SeeDeferred", func() {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.NotNil(s.T(), foundIdx)

			require.Equal(s.T(), admin_query_v1.IndexState_INDEX_STATE_DEFERRED, foundIdx.State)
		})

		s.Run("Build", func() {
			resp, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})

		s.Run("SeeBuilding", func() {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.NotNil(s.T(), foundIdx)

			require.NotEqual(s.T(), admin_query_v1.IndexState_INDEX_STATE_DEFERRED, foundIdx.State)
		})

		s.Run("Drop", func() {
			resp, err := queryAdminClient.DropIndex(context.Background(), &admin_query_v1.DropIndexRequest{
				Name:           indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})
	})
}
