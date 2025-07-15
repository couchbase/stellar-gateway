package test

import (
	"context"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func (s *GatewayOpsTestSuite) TestCreateCollection() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]
	scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	s.T().Cleanup(func() {
		deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("BucketNotFound", func() {
		colName := uuid.NewString()[:6]
		_, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     "missing-bucket",
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("ScopeNotFound", func() {
		colName := uuid.NewString()[:6]
		_, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      "missing-scope",
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("InheritBucketExpiry", func() {
		colName := uuid.NewString()[:6]
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)

		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}
	})

	s.Run("MaxExpiry", func() {
		colName := uuid.NewString()[:6]
		maxExpiry := uint32(100)
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}
	})

	s.Run("NoExpiry", func() {
		if !s.SupportsFeature(TestFeatureCollectionNoExpriy) {
			s.T().Skip()
		}

		colName := uuid.NewString()[:6]
		maxExpiry := uint32(0)
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}
	})

	s.Run("NoExpiryUnsupportedByAPIVersion", func() {
		colName := uuid.NewString()[:6]
		maxExpiry := uint32(0)
		apiVer := map[string]string{
			"X-API-Version": "2024-05-10",
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(apiVer))

		// Create collection with api version before CollectionNoExpiry should result in maxExpiry not being set
		colResp, err := colClient.CreateCollection(ctx, &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}
	})

	s.Run("HistoryRetention", func() {
		// BUG(ING-1128): HistoryRetentionEnabled is nil on listed collection
		s.T().Skip("ING-1128")

		adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)
		magmaBucket := uuid.NewString()[:6]
		resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
			BucketName:     magmaBucket,
			BucketType:     admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
			StorageBackend: ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		s.T().Cleanup(func() {
			_, _ = adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
				BucketName: magmaBucket,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
		})

		scopeName := "_default"
		colName := uuid.NewString()[:6]
		historyRetentionEnabled := true
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:              bucketName,
			ScopeName:               scopeName,
			CollectionName:          colName,
			HistoryRetentionEnabled: &historyRetentionEnabled,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.HistoryRetentionEnabled, historyRetentionEnabled)
		}
	})
}

func (s *GatewayOpsTestSuite) TestUpdateCollection() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]
	scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	colName := uuid.NewString()[:6]
	colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: colName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), colResp, err)

	s.T().Cleanup(func() {
		deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("BucketNotFound", func() {
		_, err := colClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
			BucketName:     "missing-bucket",
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("ScopeNotFound", func() {
		_, err := colClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      "missing-scope",
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("NoExpiryUnsupportedByAPIVersion", func() {
		maxExpiry := uint32(0)
		apiVer := map[string]string{
			"X-API-Version": "2024-05-10",
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(apiVer))

		// Create collection with api version before CollectionNoExpiry should result in maxExpiry not being set
		colResp, err := colClient.UpdateCollection(ctx, &admin_collection_v1.UpdateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}
	})

	s.Run("NoExpiry", func() {
		if !s.SupportsFeature(TestFeatureCollectionNoExpriy) {
			s.T().Skip()
		}

		maxExpiry := uint32(0)
		colResp, err := colClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}
	})

	s.Run("MaxExpiry", func() {
		if !s.SupportsFeature(TestFeatureCollectionNoExpriy) {
			s.T().Skip()
		}

		maxExpiry := uint32(123)
		colResp, err := colClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}
	})

	s.Run("HistoryRetention", func() {
		// BUG(ING-1128): HistoryRetentionEnabled is nil on listed collection
		s.T().Skip("ING-1128")

		adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)
		magmaBucket := uuid.NewString()[:6]
		resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
			BucketName:     magmaBucket,
			BucketType:     admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
			StorageBackend: ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		s.T().Cleanup(func() {
			_, _ = adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
				BucketName: magmaBucket,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
		})

		scopeName := "_default"

		colName := uuid.NewString()[:6]
		createResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), createResp, err)

		historyRetentionEnabled := true
		colResp, err := colClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
			BucketName:              bucketName,
			ScopeName:               scopeName,
			CollectionName:          colName,
			HistoryRetentionEnabled: &historyRetentionEnabled,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)
		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.HistoryRetentionEnabled, historyRetentionEnabled)
		}
	})
}

func (s *GatewayOpsTestSuite) TestListCollection() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]
	scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	s.T().Cleanup(func() {
		deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	// basic success case is covered whe cols are listed in create tests.

	s.Run("BucketNotFound", func() {
		_, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
			BucketName: "missing-bucket",
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("NoExpiryUnsupportedByAPIVersion", func() {
		if !s.SupportsFeature(TestFeatureCollectionNoExpriy) {
			s.T().Skip()
		}

		colName := uuid.NewString()[:6]
		maxExpiry := uint32(0)

		// Create collection with latest api version and MaxExpiry 0 should result in maxExpiry being set to 0
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)

		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}

		// List collections with apiVersion before CollectionNoExpiry should result in collection being listed without maxExpiry
		apiVer := map[string]string{
			"X-API-Version": "2024-05-10",
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(apiVer))

		found = s.findCollection(ctx, colClient, bucketName, scopeName, colName)

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}
	})
}

func (s *GatewayOpsTestSuite) TestDeleteCollection() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]
	scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	s.T().Cleanup(func() {
		deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	colName := uuid.NewString()[:6]
	colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: colName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), colResp, err)

	s.Run("BucketNotFound", func() {
		_, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     "missing-bucket",
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("ScopeNotFound", func() {
		_, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      "missing-scope",
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("MissingCollection", func() {
		_, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: "missing-col",
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("Basic", func() {
		_, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)

		found := s.findCollection(context.Background(), colClient, bucketName, scopeName, colName)
		assert.Nil(s.T(), found)
	})
}

func (s *GatewayOpsTestSuite) TestCreateScope() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]

	s.T().Cleanup(func() {
		deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("Basic", func() {
		scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), scopeResp, err)

		found := s.findScope(colClient, bucketName, scopeName)
		assert.NotNil(s.T(), found)
	})

	s.Run("DuplicateScopeName", func() {
		_, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
	})

	s.Run("BucketNotFound", func() {
		scopeName := uuid.NewString()[:6]
		_, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
			BucketName: "missing-bucket",
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})
}

func (s *GatewayOpsTestSuite) TestDeleteScope() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]
	scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	s.Run("Basic", func() {
		deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)

		found := s.findScope(colClient, bucketName, scopeName)
		assert.Nil(s.T(), found)
	})

	s.Run("AlreadyDeleted", func() {
		_, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("BucketNotFound", func() {
		_, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: "missing-bucket",
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})
}

func (s *GatewayOpsTestSuite) findScope(
	colClient admin_collection_v1.CollectionAdminServiceClient,
	bucketName string,
	scopeName string,
) *admin_collection_v1.ListCollectionsResponse_Scope {
	listResp, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
		BucketName: bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), listResp, err)

	var found *admin_collection_v1.ListCollectionsResponse_Scope
	for _, s := range listResp.Scopes {
		if s.Name == scopeName {
			found = s
		}
	}

	return found
}

func (s *GatewayOpsTestSuite) findCollection(
	ctx context.Context,
	colClient admin_collection_v1.CollectionAdminServiceClient,
	bucketName string,
	scopeName string,
	colName string,
) *admin_collection_v1.ListCollectionsResponse_Collection {
	listResp, err := colClient.ListCollections(ctx, &admin_collection_v1.ListCollectionsRequest{
		BucketName: bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), listResp, err)

	var found *admin_collection_v1.ListCollectionsResponse_Collection
	for _, s := range listResp.Scopes {
		if s.Name == scopeName {
			for _, c := range s.Collections {
				if c.Name == colName {
					found = c
				}
			}
		}
	}

	return found
}
