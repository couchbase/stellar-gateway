package test

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestQueryManagement() {
	if !s.SupportsFeature(TestFeatureQueryManagement) {
		s.T().Skip()
	}
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

		s.Run("DropMissing", func() {
			_, err := queryAdminClient.DropPrimaryIndex(context.Background(), &admin_query_v1.DropPrimaryIndexRequest{
				Name:           &indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.NotFound)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), d.ResourceType, "queryindex")
			})
		})

		s.Run("CreateAlreadyExists", func() {
			resp, err := queryAdminClient.CreatePrimaryIndex(context.Background(), &admin_query_v1.CreatePrimaryIndexRequest{
				Name:           &indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			_, err = queryAdminClient.CreatePrimaryIndex(context.Background(), &admin_query_v1.CreatePrimaryIndexRequest{
				Name:           &indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.AlreadyExists)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), d.ResourceType, "queryindex")
			})
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

		s.Run("DropMissing", func() {
			_, err := queryAdminClient.DropIndex(context.Background(), &admin_query_v1.DropIndexRequest{
				Name:           uuid.NewString()[:6],
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.NotFound)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), d.ResourceType, "queryindex")
			})
		})

		s.Run("CreateAlreadyExists", func() {
			indexName := uuid.NewString()[:6]
			resp, err := queryAdminClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
				Name:           indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
				Fields:         []string{"test"},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			_, err = queryAdminClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
				Name:           indexName,
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
				Fields:         []string{"test"},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.AlreadyExists)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), d.ResourceType, "queryindex")
			})
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

			var found bool
			for _, index := range resp.Indexes {
				if index.Name == indexName &&
					index.BucketName == s.bucketName &&
					index.GetScopeName() == s.scopeName &&
					index.GetCollectionName() == s.collectionName {
					found = true
				}
			}

			require.True(s.T(), found, "Did not find index %s in list of deferred indexes %v", indexName, resp.Indexes)
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

func (s *GatewayOpsTestSuite) TestQueryManagementBuildDeferredBuildsAllInBucket() {
	if !s.SupportsFeature(TestFeatureQueryManagement) {
		s.T().Skip()
	}

	queryAdminClient := admin_query_v1.NewQueryAdminServiceClient(s.gatewayConn)

	scope1 := uuid.NewString()[:6]
	collection := uuid.NewString()[:6]
	scope2 := uuid.NewString()[:6]
	defaultScope := "_default"
	defaultCollection := "_default"

	deleteScope1 := s.CreateScope(s.bucketName, scope1)
	defer deleteScope1()

	deleteScope2 := s.CreateScope(s.bucketName, scope2)
	defer deleteScope2()

	s.CreateCollection(s.bucketName, scope1, collection)
	s.CreateCollection(s.bucketName, scope2, collection)

	indexName := uuid.NewString()
	trueBool := true

	createIndex := func(scopeName, collectionName string) {
		iResp, err := queryAdminClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
			Name:           indexName,
			BucketName:     s.bucketName,
			ScopeName:      &scopeName,
			CollectionName: &collectionName,
			Fields:         []string{"test"},
			Deferred:       &trueBool,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), iResp, err)
	}

	createIndex(scope1, collection)
	createIndex(scope2, collection)
	createIndex(defaultScope, defaultCollection)

	// Specifying no scope or collection should build all indexes in the bucket.
	buildResp, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName: s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), buildResp, err)

	findIndex := func(indexName, bucketName, scopeName, collectionName string) bool {
		for _, index := range buildResp.Indexes {
			if index.Name == indexName &&
				index.BucketName == bucketName &&
				index.GetScopeName() == scopeName &&
				index.GetCollectionName() == collectionName {
				return true
			}
		}

		return false
	}

	found := findIndex(indexName, s.bucketName, scope1, collection)
	require.True(s.T(), found, "Did not find index %s.%s.%s.%s in list of deferred indexes %v",
		s.bucketName, scope1, collection, indexName, buildResp.Indexes)

	found = findIndex(indexName, s.bucketName, scope2, collection)
	require.True(s.T(), found, "Did not find index %s.%s.%s.%s in list of deferred indexes %v",
		s.bucketName, scope2, collection, indexName, buildResp.Indexes)

	found = findIndex(indexName, s.bucketName, defaultScope, defaultCollection)
	require.True(s.T(), found, "Did not find index %s.%s.%s.%s in list of deferred indexes %v",
		s.bucketName, defaultScope, defaultCollection, indexName, buildResp.Indexes)
}

func (s *GatewayOpsTestSuite) TestQueryManagementBuildDeferredBuildsSpecificCollection() {
	if !s.SupportsFeature(TestFeatureQueryManagement) {
		s.T().Skip()
	}

	queryAdminClient := admin_query_v1.NewQueryAdminServiceClient(s.gatewayConn)

	scope1 := uuid.NewString()[:6]
	collection := uuid.NewString()[:6]
	scope2 := uuid.NewString()[:6]
	defaultScope := "_default"
	defaultCollection := "_default"

	deleteScope1 := s.CreateScope(s.bucketName, scope1)
	defer deleteScope1()

	deleteScope2 := s.CreateScope(s.bucketName, scope2)
	defer deleteScope2()

	s.CreateCollection(s.bucketName, scope1, collection)
	s.CreateCollection(s.bucketName, scope2, collection)

	indexName := uuid.NewString()
	trueBool := true

	createIndex := func(scopeName, collectionName string) {
		iResp, err := queryAdminClient.CreateIndex(context.Background(), &admin_query_v1.CreateIndexRequest{
			Name:           indexName,
			BucketName:     s.bucketName,
			ScopeName:      &scopeName,
			CollectionName: &collectionName,
			Fields:         []string{"test"},
			Deferred:       &trueBool,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), iResp, err)
	}

	createIndex(scope1, collection)
	createIndex(scope2, collection)
	createIndex(defaultScope, defaultCollection)

	// Specifying no scope or collection should build all indexes in the bucket.
	buildResp, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName:     s.bucketName,
		ScopeName:      &scope1,
		CollectionName: &collection,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), buildResp, err)

	findIndex := func(indexName, bucketName, scopeName, collectionName string) bool {
		for _, index := range buildResp.Indexes {
			if index.Name == indexName &&
				index.BucketName == bucketName &&
				index.GetScopeName() == scopeName &&
				index.GetCollectionName() == collectionName {
				return true
			}
		}

		return false
	}

	require.Len(s.T(), buildResp.Indexes, 1)

	found := findIndex(indexName, s.bucketName, scope1, collection)
	require.True(s.T(), found, "Did not find index %s.%s.%s.%s in list of deferred indexes %v",
		s.bucketName, scope1, collection, indexName, buildResp.Indexes)
}
