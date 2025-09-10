package test

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type commonIndexErrorTestData struct {
	IndexName  string
	BucketName *string
	ScopeName  *string
	Creds      credentials.PerRPCCredentials
}

func (s *GatewayOpsTestSuite) RunCommonIndexErrorCases(
	ctx context.Context,
	indexName string,
	fn func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error),
) {
	/*bucket*/
	_, scope := s.initialiseBucketAndScope()

	s.Run("BucketNotFound", func() {
		_, err := fn(ctx, &commonIndexErrorTestData{
			IndexName:  indexName,
			BucketName: ptr.To("missing-bucket"),
			ScopeName:  scope,
			Creds:      s.basicRpcCreds,
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "bucket", d.ResourceType)
		})
	})

	// TODO - ING-1186
	// s.Run("ScopeNamedWithoutBucket", func() {
	// 	_, err := fn(ctx, &commonIndexErrorTestData{
	// 		IndexName:  indexName,
	// 		BucketName: nil,
	// 		ScopeName:  ptr.To("some-scope"),
	// 		Creds:      s.basicRpcCreds,
	// 	})
	// 	assertRpcStatus(s.T(), err, codes.InvalidArgument)
	// 	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
	// 		assert.Equal(s.T(), "scope", d.ResourceType)
	// 	})
	// })

	// TODO - ING-1187
	// s.Run("BadCredentials", func() {
	// 	_, err := fn(ctx, &commonIndexErrorTestData{
	// 		IndexName:  indexName,
	// 		BucketName: bucket,
	// 		ScopeName:  scope,
	// 		Creds:      s.badRpcCreds,
	// 	})
	// 	assertRpcStatus(s.T(), err, codes.Unauthenticated)
	// 	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
	// 		assert.Equal(s.T(), "searchindex", d.ResourceType)
	// 	})
	// })
}

func (s *GatewayOpsTestSuite) TestGetIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type getTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	getTests := []getTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1151
		// {
		// 	description: "IndexNameBlank",
		// 	modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
		// 		def.Name = ""
		// 		return def
		// 	},
		// 	resourceDetails: "search-index",
		// 	expect:          codes.InvalidArgument,
		// },
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
		{
			description: "EmptyScopeName",
			modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
				def.ScopeName = nil
				return def
			},
			expect: codes.OK,
		},
	}

	for i := range getTests {
		t := getTests[i]
		s.Run(t.description, func() {
			defaultGetRequest := admin_search_v1.GetIndexRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultGetRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.GetIndex(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				s.Assert().Equal(indexName, resp.Index.Name)
				s.Assert().Equal("fulltext-index", resp.Index.Type)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.GetIndex(ctx, &admin_search_v1.GetIndexRequest{
				Name:       opts.IndexName,
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestListIndexes() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type listTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	listTests := []listTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest {
				return def
			},
			expect: codes.OK,
		},
	}

	for i := range listTests {
		t := listTests[i]
		s.Run(t.description, func() {
			defaultListRequest := admin_search_v1.ListIndexesRequest{
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultListRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.ListIndexes(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				s.Assert().Equal(1, len(resp.Indexes))
				s.Assert().Equal(indexName, resp.Indexes[0].Name)
				s.Assert().Equal("fulltext-index", resp.Indexes[0].Type)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.ListIndexes(ctx, &admin_search_v1.ListIndexesRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestCreateIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	bucket, scope := s.initialiseBucketAndScope()

	sourceType := "couchbase"

	planParams := map[string]interface{}{
		"test":  map[string]interface{}{},
		"test2": map[string]interface{}{},
	}
	b, err := json.Marshal(planParams)
	s.Require().NoError(err)

	basicIndexName := newIndexName()
	createdIndexes := &[]string{}

	s.T().Cleanup(func() {
		defer cancel()
		for _, indexName := range *createdIndexes {
			_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
		}
	})

	type createTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest
		resourceDetails string
		expect          codes.Code
		creds           *credentials.PerRPCCredentials
	}

	createTests := []createTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "WithPlanParams",
			modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
				def.Name = newIndexName()
				def.PlanParams = map[string][]byte{
					"targets": b,
				}
				return def
			},
			expect: codes.OK,
		},
		{
			description: "WithSourceParams",
			modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
				def.Name = newIndexName()
				def.SourceParams = map[string][]byte{
					"targets": b,
				}
				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1134
		// {
		// 	description: "BlankBucketName",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = newIndexName()
		// 		def.BucketName = ptr.To("")
		// 		return def
		// 	},
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1148
		// {
		// 	description: "ScopeNamedWithoutBucket",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = newIndexName()
		// 		def.ScopeName = ptr.To("some-scope")
		// 		return def
		// 	},
		// 	resourceDetails: "scope",
		// 	expect:          codes.InvalidArgument,
		// },
		// TODO - ING-1144
		// {
		// 	description: "SpecialCharInIndexName",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = "special-char-name!"
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1145
		// {
		// 	description: "IndexNameTooLong",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = s.docIdOfLen(260)
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1147
		// {
		// 	description: "InvalidSourceType",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.SourceType = ptr.To("not-a-source-type")
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.InvalidArgument,
		// },
		//
		// TODO - ING-1149
		// {
		// 	description: "NamedSourceMissing",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.SourceName = ptr.To("some-bucket")
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.NotFound || codes.InvalidArgument,
		// },
		// TODO - ING-1150
		// {
		// 	description: "InvalidType",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Type = "not-a-type"
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.InvalidArgument,
		// },
		{
			description: "AlreadyExists",
			modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.AlreadyExists,
		},
	}

	for i := range createTests {
		t := createTests[i]
		s.Run(t.description, func() {
			defaultCreateReq := admin_search_v1.CreateIndexRequest{
				Name:       basicIndexName,
				BucketName: bucket,
				ScopeName:  scope,
				Type:       "fulltext-index",
				SourceType: &sourceType,
				SourceName: &s.bucketName,
			}
			req := t.modifyDefault(&defaultCreateReq)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.CreateIndex(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				*createdIndexes = append(*createdIndexes, req.Name)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		basicIndexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestUpdateIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	params := map[string]interface{}{
		"test":  map[string]interface{}{},
		"test2": map[string]interface{}{},
	}
	b, err := json.Marshal(params)
	s.Require().NoError(err)

	type updateTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	updateTests := []updateTest{
		{
			description: "SourceType",
			modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
				def.Index.SourceType = ptr.To("gocbcore")
				return def
			},
			expect: codes.OK,
		},
		{
			description: "Params",
			modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
				def.Index.Params = map[string][]byte{
					"targets": b,
				}

				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1185
		{
			description: "SourceParams",
			modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
				def.Index.SourceParams = map[string][]byte{
					"targets": b,
				}

				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1155
		// {
		// 	description: "PlanParams",
		// 	modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
		// 		def.Index.PlanParams = map[string][]byte{
		// 			"targets": b,
		// 		}

		// 		return def
		// 	},
		// 	expect: codes.OK,
		// },
		// TODO - ING-1156
		// {
		// 	description: "InvalidSourceType",
		// 	modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
		// 		def.Index.SourceType = ptr.To("notASourceType")
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1157
		// {
		// 	description: "SourceNameNotFound",
		// 	modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
		// 		def.Index.SourceName = ptr.To("missing-source")
		// 		return def
		// 	},
		//  resourceDetails: "searchindex",
		// 	expect: codes.NotFound,
		// },
		// TODO - ING-1558
		// {
		// 	description: "IndexNotFound",
		// 	modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
		// 		def.Index.Name = "missing-index"
		// 		return def
		// 	},
		// 	resourceDetails: "searchindex",
		// 	expect:          codes.NotFound,
		// },
		{
			description: "UuidMismatch",
			modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
				def.Index.Uuid = "123456"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.Aborted,
		},
	}

	for i := range updateTests {
		t := updateTests[i]
		s.Run(t.description, func() {
			getResp, err := searchAdminClient.GetIndex(ctx, &admin_search_v1.GetIndexRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			index := getResp.Index

			defaultUpdateRequest := admin_search_v1.UpdateIndexRequest{
				Index:      index,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultUpdateRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.UpdateIndex(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)

				updatedGetResp, err := searchAdminClient.GetIndex(ctx, &admin_search_v1.GetIndexRequest{
					Name:       indexName,
					BucketName: bucket,
					ScopeName:  scope,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)

				assert.Equal(s.T(), req.Index.SourceParams, updatedGetResp.Index.SourceParams)
				assert.Equal(s.T(), req.Index.PlanParams, updatedGetResp.Index.PlanParams)
				assert.Equal(s.T(), req.Index.SourceName, updatedGetResp.Index.SourceName)
				assert.Equal(s.T(), req.Index.SourceType, updatedGetResp.Index.SourceType)
				assert.Equal(s.T(), req.Index.SourceUuid, updatedGetResp.Index.SourceUuid)
				assert.Equal(s.T(), req.Index.Type, updatedGetResp.Index.Type)

				// Wait before next test so that previous update can take effect
				time.Sleep(time.Second * 5)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.UpdateIndex(ctx, &admin_search_v1.UpdateIndexRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestDeleteIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type deleteTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	deleteTests := []deleteTest{
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
		// TODO-ING-1164
		// {
		// 	description: "SpecialCharsInIndexName",
		// 	modifyDefault: func(def *admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest {
		// 		def.Name = "special-char-name@£$%"
		// 		return def
		// 	},
		// 	resourceDetails: "searchindex",
		// 	expect:          codes.InvalidArgument,
		// },
		{
			description: "Success",
			modifyDefault: func(def *admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest {
				return def
			},
			expect: codes.OK,
		},
	}

	for i := range deleteTests {
		t := deleteTests[i]
		s.Run(t.description, func() {
			defaultDeleteRequest := admin_search_v1.DeleteIndexRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultDeleteRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.DeleteIndex(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)

				_, err := searchAdminClient.GetIndex(ctx, &admin_search_v1.GetIndexRequest{
					Name:       indexName,
					BucketName: bucket,
					ScopeName:  scope,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				assertRpcStatus(s.T(), err, codes.NotFound)

				resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
					Name:       indexName,
					BucketName: bucket,
					ScopeName:  scope,
					Type:       "fulltext-index",
					SourceType: ptr.To("couchbase"),
					SourceName: &s.bucketName,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), resp, err)

				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestPauseIndexIngest() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type pauseTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	pauseTests := []pauseTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "AlreadyPaused",
			modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range pauseTests {
		t := pauseTests[i]
		s.Run(t.description, func() {
			defaultPauseRequest := admin_search_v1.PauseIndexIngestRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultPauseRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.PauseIndexIngest(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.PauseIndexIngest(ctx, &admin_search_v1.PauseIndexIngestRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestResumeIndexIngest() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	resp, err := searchAdminClient.PauseIndexIngest(ctx, &admin_search_v1.PauseIndexIngestRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	type resumeTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	resumeTests := []resumeTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "AlreadyResumed",
			modifyDefault: func(def *admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range resumeTests {
		t := resumeTests[i]
		s.Run(t.description, func() {
			defaultResumeRequest := admin_search_v1.ResumeIndexIngestRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultResumeRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.ResumeIndexIngest(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.ResumeIndexIngest(ctx, &admin_search_v1.ResumeIndexIngestRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestGetIndexedDocCount() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type indexedCountTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.GetIndexedDocumentsCountRequest) *admin_search_v1.GetIndexedDocumentsCountRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	indexedCountTests := []indexedCountTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.GetIndexedDocumentsCountRequest) *admin_search_v1.GetIndexedDocumentsCountRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.GetIndexedDocumentsCountRequest) *admin_search_v1.GetIndexedDocumentsCountRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range indexedCountTests {
		t := indexedCountTests[i]
		s.Run(t.description, func() {
			defaultIndexedCountRequest := admin_search_v1.GetIndexedDocumentsCountRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultIndexedCountRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.GetIndexedDocumentsCount(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.GetIndexedDocumentsCount(ctx, &admin_search_v1.GetIndexedDocumentsCountRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestAnalyzeDocument() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	testDoc := `{"someText": "Analyze this please"}`
	docBytes, err := json.Marshal(testDoc)
	require.NoError(s.T(), err)

	type analyzeTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.AnalyzeDocumentRequest) *admin_search_v1.AnalyzeDocumentRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	analyzeTests := []analyzeTest{
		// TODO - ING-1174
		// {
		// 	description: "Basic",
		// 	modifyDefault: func(def *admin_search_v1.AnalyzeDocumentRequest) *admin_search_v1.AnalyzeDocumentRequest {
		// 		return def
		// 	},
		// 	expect: codes.OK,
		// },
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.AnalyzeDocumentRequest) *admin_search_v1.AnalyzeDocumentRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range analyzeTests {
		t := analyzeTests[i]
		s.Run(t.description, func() {
			defaultAnalyzeRequest := admin_search_v1.AnalyzeDocumentRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
				Doc:        docBytes,
			}
			req := t.modifyDefault(&defaultAnalyzeRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.AnalyzeDocument(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.AnalyzeDocument(ctx, &admin_search_v1.AnalyzeDocumentRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestFreezeIndexPlan() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type freezeTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.FreezeIndexPlanRequest) *admin_search_v1.FreezeIndexPlanRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	freezeTests := []freezeTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.FreezeIndexPlanRequest) *admin_search_v1.FreezeIndexPlanRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "AlreadyFrozen",
			modifyDefault: func(def *admin_search_v1.FreezeIndexPlanRequest) *admin_search_v1.FreezeIndexPlanRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.FreezeIndexPlanRequest) *admin_search_v1.FreezeIndexPlanRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range freezeTests {
		t := freezeTests[i]
		s.Run(t.description, func() {
			defaultFreezeRequest := admin_search_v1.FreezeIndexPlanRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultFreezeRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.FreezeIndexPlan(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.FreezeIndexPlan(ctx, &admin_search_v1.FreezeIndexPlanRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestUnfreezeIndexPlan() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	resp, err := searchAdminClient.FreezeIndexPlan(ctx, &admin_search_v1.FreezeIndexPlanRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	type unfreezeTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.UnfreezeIndexPlanRequest) *admin_search_v1.UnfreezeIndexPlanRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	freezeTests := []unfreezeTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.UnfreezeIndexPlanRequest) *admin_search_v1.UnfreezeIndexPlanRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "NotFrozen",
			modifyDefault: func(def *admin_search_v1.UnfreezeIndexPlanRequest) *admin_search_v1.UnfreezeIndexPlanRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.UnfreezeIndexPlanRequest) *admin_search_v1.UnfreezeIndexPlanRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range freezeTests {
		t := freezeTests[i]
		s.Run(t.description, func() {
			defaultUnfreezeRequest := admin_search_v1.UnfreezeIndexPlanRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultUnfreezeRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.UnfreezeIndexPlan(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.UnfreezeIndexPlan(ctx, &admin_search_v1.UnfreezeIndexPlanRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestAllowIndexQuerying() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	resp, err := searchAdminClient.DisallowIndexQuerying(ctx, &admin_search_v1.DisallowIndexQueryingRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	type allowTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.AllowIndexQueryingRequest) *admin_search_v1.AllowIndexQueryingRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	allowTests := []allowTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.AllowIndexQueryingRequest) *admin_search_v1.AllowIndexQueryingRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "AlreadyAllowed",
			modifyDefault: func(def *admin_search_v1.AllowIndexQueryingRequest) *admin_search_v1.AllowIndexQueryingRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.AllowIndexQueryingRequest) *admin_search_v1.AllowIndexQueryingRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range allowTests {
		t := allowTests[i]
		s.Run(t.description, func() {
			defaultAllowRequest := admin_search_v1.AllowIndexQueryingRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultAllowRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.AllowIndexQuerying(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.AllowIndexQuerying(ctx, &admin_search_v1.AllowIndexQueryingRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestDisallowIndexQuerying() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	bucket, scope := s.initialiseBucketAndScope()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	indexName := s.newIndex(ctx, cancel, bucket, scope, searchAdminClient)

	type disallowTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.DisallowIndexQueryingRequest) *admin_search_v1.DisallowIndexQueryingRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	disallowTests := []disallowTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.DisallowIndexQueryingRequest) *admin_search_v1.DisallowIndexQueryingRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "AlreadyDisallowed",
			modifyDefault: func(def *admin_search_v1.DisallowIndexQueryingRequest) *admin_search_v1.DisallowIndexQueryingRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.DisallowIndexQueryingRequest) *admin_search_v1.DisallowIndexQueryingRequest {
				def.Name = "missing-index"
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.NotFound,
		},
	}

	for i := range disallowTests {
		t := disallowTests[i]
		s.Run(t.description, func() {
			defaultDisallowRequest := admin_search_v1.DisallowIndexQueryingRequest{
				Name:       indexName,
				BucketName: bucket,
				ScopeName:  scope,
			}
			req := t.modifyDefault(&defaultDisallowRequest)
			creds := s.basicRpcCreds
			if t.creds != nil {
				creds = *t.creds
			}

			resp, err := searchAdminClient.DisallowIndexQuerying(ctx, req, grpc.PerRPCCredentials(creds))
			if t.expect == codes.OK {
				require.NoError(s.T(), err)
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}

	s.RunCommonIndexErrorCases(
		ctx,
		indexName,
		func(ctx context.Context, opts *commonIndexErrorTestData) (interface{}, error) {
			return searchAdminClient.DisallowIndexQuerying(ctx, &admin_search_v1.DisallowIndexQueryingRequest{
				BucketName: opts.BucketName,
				ScopeName:  opts.ScopeName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func newIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func (s *GatewayOpsTestSuite) initialiseBucketAndScope() (*string, *string) {
	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	return bucket, scope
}

func (s *GatewayOpsTestSuite) newIndex(
	ctx context.Context,
	cancel context.CancelFunc,
	bucket, scope *string,
	client admin_search_v1.SearchAdminServiceClient) string {
	indexName := newIndexName()

	s.T().Cleanup(func() {
		defer cancel()
		_, _ = client.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	resp, err := client.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	return indexName
}
