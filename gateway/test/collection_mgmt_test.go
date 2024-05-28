package test

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (s *GatewayOpsTestSuite) TestCollectionManagement() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)

	bucketName := "default"
	scopeName := uuid.NewString()[:6]
	scopeResp, err := colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	s.Run("CreateInheritBucketExpiry", func() {
		colName := uuid.NewString()[:6]
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)

		listResp, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
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

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}

		deleteResp, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("CreateWithMaxExpiry", func() {
		colName := uuid.NewString()[:6]
		maxExpiry := uint32(100)
		colResp, err := colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
			MaxExpirySecs:  &maxExpiry,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), colResp, err)

		listResp, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
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

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}

		deleteResp, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("CreateNoExpiryUnsupportedByAPIVersion", func() {
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

		// List collections with latest apiVersion to make sure create did not set expiry to -1
		listResp, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
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

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}

		deleteResp, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("CreateWithNoExpiry", func() {
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

		listResp, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
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

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}

		deleteResp, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	s.Run("ListNoExpiryUnsupportedByAPIVersion", func() {
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

		listResp, err := colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
			BucketName: bucketName,
		},
			grpc.PerRPCCredentials(s.basicRpcCreds))
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

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Equal(s.T(), *found.MaxExpirySecs, maxExpiry)
		}

		// List collections with apiVersion before CollectionNoExpiry should result in collection being listed without maxExpiry
		apiVer := map[string]string{
			"X-API-Version": "2024-05-10",
		}
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(apiVer))

		listResp, err = colClient.ListCollections(ctx, &admin_collection_v1.ListCollectionsRequest{
			BucketName: bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), listResp, err)

		found = nil
		for _, s := range listResp.Scopes {
			if s.Name == scopeName {
				for _, c := range s.Collections {
					if c.Name == colName {
						found = c
					}
				}
			}
		}

		if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
			assert.Nil(s.T(), found.MaxExpirySecs)
		}

		deleteResp, err := colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucketName,
			ScopeName:      scopeName,
			CollectionName: colName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})

	deleteResp, err := colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))

	requireRpcSuccess(s.T(), deleteResp, err)
}
