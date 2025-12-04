package test

import (
	"context"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type commonColMgmtErrorTestCaseData struct {
	BucketName string
	Creds      credentials.PerRPCCredentials
}

func (s *GatewayOpsTestSuite) RunCommonColMgmtErrorCases(
	fn func(opts *commonColMgmtErrorTestCaseData) (interface{}, error),
	resource string,
) {
	s.Run("BucketNotFound", func() {
		_, err := fn(&commonColMgmtErrorTestCaseData{
			Creds:      s.badRpcCreds,
			BucketName: "missing-bucket",
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "bucket", d.ResourceType)
		})
	})
	s.Run("BadCredentials", func() {
		_, err := fn(&commonColMgmtErrorTestCaseData{
			BucketName: "default",
			Creds:      s.badRpcCreds,
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), resource, d.ResourceType)
		})
	})
	s.Run("NoPermissions", func() {
		_, err := fn(&commonColMgmtErrorTestCaseData{
			BucketName: "default",
			Creds:      s.getNoPermissionRpcCreds(),
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), resource, d.ResourceType)
		})
	})
}

func (s *GatewayOpsTestSuite) TestCreateCollection() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]

	s.createScope(bucketName, scopeName, colClient)

	magmaBucket := s.createMagmaBucket()

	type createTest struct {
		description   string
		modifyDefault func(*admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest
		getCreds      func() credentials.PerRPCCredentials
		context       *context.Context
		checkFn       func(*admin_collection_v1.ListCollectionsResponse_Collection)
		expect        codes.Code
	}

	createTests := []createTest{
		{
			description: "ScopeNotFound",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				def.ScopeName = "missing-scope"
				return def
			},
			expect: codes.NotFound,
		},
		{
			description: "InheritBucketExpiry",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				assert.Nil(s.T(), col.MaxExpirySecs)
			},
		},
		{
			description: "MaxExpiry",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				maxExpiry := uint32(100)
				def.MaxExpirySecs = &maxExpiry
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				assert.Equal(s.T(), uint32(100), *col.MaxExpirySecs)
			},
		},
		{
			description: "NoExpiry",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				maxExpiry := uint32(0)
				def.MaxExpirySecs = &maxExpiry
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				assert.Equal(s.T(), uint32(0), *col.MaxExpirySecs)
			},
		},
		{
			description: "NoExpiryUnsupportedByAPIVersion",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				maxExpiry := uint32(0)
				def.MaxExpirySecs = &maxExpiry
				return def
			},
			context: ptr.To(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
				"X-API-Version": "2024-05-10",
			}))),
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				// Create collection with api version before CollectionNoExpiry
				// should result in maxExpiry not being set
				assert.Nil(s.T(), col.MaxExpirySecs)
			},
		},
		{
			description: "HistoryRetention",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				def.BucketName = magmaBucket
				def.ScopeName = "_default"
				def.HistoryRetentionEnabled = ptr.To(true)
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				if assert.NotNil(s.T(), col.HistoryRetentionEnabled, "HistoryRetentionEnabled should not be nil") {
					assert.True(s.T(), *col.HistoryRetentionEnabled)
				}
			},
		},
		{
			description: "ReadOnlyUser",
			modifyDefault: func(def *admin_collection_v1.CreateCollectionRequest) *admin_collection_v1.CreateCollectionRequest {
				return def
			},
			getCreds: s.getReadOnlyRpcCredentials,
			expect:   codes.PermissionDenied,
		},
	}

	for i := range createTests {
		t := createTests[i]
		s.Run(t.description, func() {
			if t.description == "NoExpiry" && !s.SupportsFeature(TestFeatureCollectionNoExpriy) {
				s.T().Skip()
			}

			colName := uuid.NewString()[:6]
			defaultCreateRequest := &admin_collection_v1.CreateCollectionRequest{
				BucketName:     bucketName,
				ScopeName:      scopeName,
				CollectionName: colName,
			}
			req := t.modifyDefault(defaultCreateRequest)

			creds := s.basicRpcCreds
			if t.getCreds != nil {
				creds = t.getCreds()
			}

			ctx := context.Background()
			if t.context != nil {
				ctx = *t.context
			}

			resp, err := colClient.CreateCollection(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				requireRpcSuccess(s.T(), resp, err)

				found := s.findCollection(context.Background(), colClient, req.BucketName, req.ScopeName, colName)

				if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
					t.checkFn(found)
				}
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
		})
	}

	s.RunCommonColMgmtErrorCases(
		func(opts *commonColMgmtErrorTestCaseData) (interface{}, error) {
			return colClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
				BucketName:     opts.BucketName,
				ScopeName:      scopeName,
				CollectionName: "new-collection",
			},
				grpc.PerRPCCredentials(opts.Creds))
		},
		"collection",
	)
}

func (s *GatewayOpsTestSuite) TestUpdateCollection() {
	colClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)
	bucketName := "default"
	scopeName := uuid.NewString()[:6]

	s.createScope(bucketName, scopeName, colClient)
	colName := s.createCollection(bucketName, scopeName, colClient)

	magmaBucket := s.createMagmaBucket()
	magmaCol := s.createCollection(magmaBucket, "_default", colClient)

	type updateTest struct {
		description   string
		modifyDefault func(*admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest
		getCreds      func() credentials.PerRPCCredentials
		context       *context.Context
		checkFn       func(*admin_collection_v1.ListCollectionsResponse_Collection)
		expect        codes.Code
	}

	updateTests := []updateTest{
		{
			description: "ScopeNotFound",
			modifyDefault: func(def *admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest {
				def.ScopeName = "missing-scope"
				return def
			},
			expect: codes.NotFound,
		},
		{
			description: "NoExpiryUnsupportedByAPIVersion",
			modifyDefault: func(def *admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest {
				def.MaxExpirySecs = ptr.To(uint32(0))
				return def
			},
			context: ptr.To(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
				"X-API-Version": "2024-05-10",
			}))),
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				assert.Nil(s.T(), col.MaxExpirySecs)
			},
		},
		{
			description: "NoExpiry",
			modifyDefault: func(def *admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest {
				def.MaxExpirySecs = ptr.To(uint32(0))
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				assert.Equal(s.T(), uint32(0), *col.MaxExpirySecs)
			},
		},
		{
			description: "MaxExpiry",
			modifyDefault: func(def *admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest {
				def.MaxExpirySecs = ptr.To(uint32(123))
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				assert.Equal(s.T(), uint32(123), *col.MaxExpirySecs)
			},
		},
		{
			description: "HistoryRetention",
			modifyDefault: func(def *admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest {
				def.BucketName = magmaBucket
				def.ScopeName = "_default"
				def.CollectionName = magmaCol
				def.HistoryRetentionEnabled = ptr.To(true)
				return def
			},
			expect: codes.OK,
			checkFn: func(col *admin_collection_v1.ListCollectionsResponse_Collection) {
				if assert.NotNil(s.T(), col.HistoryRetentionEnabled, "HistoryRetentionEnabled should not be nil") {
					assert.True(s.T(), *col.HistoryRetentionEnabled)
				}
			},
		},
		{
			description: "ReadOnlyUser",
			modifyDefault: func(def *admin_collection_v1.UpdateCollectionRequest) *admin_collection_v1.UpdateCollectionRequest {
				return def
			},
			getCreds: s.getReadOnlyRpcCredentials,
			expect:   codes.PermissionDenied,
		},
	}

	for i := range updateTests {
		t := updateTests[i]
		s.Run(t.description, func() {
			if !s.SupportsFeature(TestFeatureCollectionNoExpriy) && (t.description == "MaxExpiry" || t.description == "NoExpiry") {
				s.T().Skip()
			}

			defaultUpdateRequest := &admin_collection_v1.UpdateCollectionRequest{
				BucketName:     bucketName,
				ScopeName:      scopeName,
				CollectionName: colName,
			}

			creds := s.basicRpcCreds
			if t.getCreds != nil {
				creds = t.getCreds()
			}

			ctx := context.Background()
			if t.context != nil {
				ctx = *t.context
			}

			req := t.modifyDefault(defaultUpdateRequest)

			resp, err := colClient.UpdateCollection(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				requireRpcSuccess(s.T(), resp, err)

				found := s.findCollection(context.Background(), colClient, req.BucketName, req.ScopeName, req.CollectionName)

				if assert.NotNil(s.T(), found, "Did not find collection on cluster") {
					t.checkFn(found)
				}
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
		})
	}

	s.RunCommonColMgmtErrorCases(
		func(opts *commonColMgmtErrorTestCaseData) (interface{}, error) {
			return colClient.UpdateCollection(context.Background(), &admin_collection_v1.UpdateCollectionRequest{
				BucketName:     opts.BucketName,
				ScopeName:      scopeName,
				CollectionName: colName,
			},
				grpc.PerRPCCredentials(opts.Creds))
		},
		"collection",
	)
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
			assert.Equal(s.T(), maxExpiry, *found.MaxExpirySecs)
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

	s.RunCommonColMgmtErrorCases(
		func(opts *commonColMgmtErrorTestCaseData) (interface{}, error) {
			return colClient.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
				BucketName: opts.BucketName,
			}, grpc.PerRPCCredentials(opts.Creds))
		},
		"collection",
	)
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

	s.RunCommonColMgmtErrorCases(
		func(opts *commonColMgmtErrorTestCaseData) (interface{}, error) {
			return colClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
				BucketName:     opts.BucketName,
				ScopeName:      scopeName,
				CollectionName: colName,
			}, grpc.PerRPCCredentials(opts.Creds))
		},
		"collection",
	)
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

	s.RunCommonColMgmtErrorCases(
		func(opts *commonColMgmtErrorTestCaseData) (interface{}, error) {
			return colClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
				BucketName: opts.BucketName,
				ScopeName:  scopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		},
		"scope",
	)
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

	s.RunCommonColMgmtErrorCases(
		func(opts *commonColMgmtErrorTestCaseData) (interface{}, error) {
			return colClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
				BucketName: opts.BucketName,
				ScopeName:  scopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		},
		"scope",
	)
}

func (s *GatewayOpsTestSuite) createMagmaBucket() string {
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

	return magmaBucket
}

func (s *GatewayOpsTestSuite) createScope(
	bucketName, scopeName string,
	client admin_collection_v1.CollectionAdminServiceClient) {

	scopeResp, err := client.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucketName,
		ScopeName:  scopeName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), scopeResp, err)

	s.T().Cleanup(func() {
		deleteResp, err := client.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucketName,
			ScopeName:  scopeName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), deleteResp, err)
	})
}

func (s *GatewayOpsTestSuite) createCollection(
	bucket, scope string,
	client admin_collection_v1.CollectionAdminServiceClient) string {

	colName := uuid.NewString()[:6]
	createResp, err := client.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: colName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), createResp, err)

	return colName
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
