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

func newIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func (s *GatewayOpsTestSuite) TestGetIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	indexName := newIndexName()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	s.T().Cleanup(func() {
		defer cancel()
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

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
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
				def.BucketName = ptr.To("missing-bucket")
				return def
			},
			resourceDetails: "bucket",
			expect:          codes.NotFound,
		},
		// TODO -ING-1152
		// {
		// 	description: "ScopeNamedWithoutBucket",
		// 	modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
		// 		def.ScopeName = ptr.To("_defualt")
		// 		return def
		// 	},
		//  resourceDetails: "scope",
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1153
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
		// },
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
}

func (s *GatewayOpsTestSuite) TestListIndexes() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	indexName := newIndexName()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	s.T().Cleanup(func() {
		defer cancel()
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	type listTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	listTests := []listTest{
		{
			description: "Basic",
			modifyDefault: func(def *admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest {
				return def
			},
			expect: codes.OK,
		},
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest {
				def.BucketName = ptr.To("missing-bucket")
				return def
			},
			resourceDetails: "bucket",
			expect:          codes.NotFound,
		},
		// TODO - ING-1162
		// {
		// 	description: "ScopeNamedWithoutBucket",
		// 	modifyDefault: func(def *admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest {
		// 		def.ScopeName = ptr.To("_default")
		// 		return def
		// 	},
		// 	resourceDetails: "scope",
		// 	expect:          codes.InvaliArgument,
		// },
		// TODO - ING-1163
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.ListIndexesRequest) *admin_search_v1.ListIndexesRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
		// },
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
}

func (s *GatewayOpsTestSuite) TestCreateIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

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

	type response struct {
		Status string `json:"status"`
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
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
				def.Name = newIndexName()
				def.BucketName = ptr.To("missing-bucket")
				return def
			},
			resourceDetails: "bucket",
			expect:          codes.NotFound,
		},
		// TODO - ING-1148
		// {
		// 	description: "ScopeNotFound",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = newIndexName()
		// 		def.ScopeName = ptr.To("missing-scope")
		// 		return def
		// 	},
		//  resourceDetails: "scope",
		// 	expect: codes.NotFound,
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
		// TODO - ING-1154
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
		// },
		{
			description: "AlreadyExists",
			modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
				return def
			},
			resourceDetails: "searchindex",
			expect:          codes.AlreadyExists,
		},
		// TODO - ING-1170
		// {
		// 	description: "ScopeNamedWithoutBucket",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.ScopeName = ptr.To("_default")
		// 		return def
		// 	},
		// 	expect: codes.InvalidArgument,
		// },
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
}

func (s *GatewayOpsTestSuite) TestUpdateIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	indexName := newIndexName()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	s.T().Cleanup(func() {
		defer cancel()
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	params := map[string]interface{}{
		"test":  map[string]interface{}{},
		"test2": map[string]interface{}{},
	}
	b, err := json.Marshal(params)
	s.Require().NoError(err)

	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

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
		//  resourceDetails: "searchindex",
		// 	expect: codes.NotFound,
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
		// TODO - ING-1159
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.UpdateIndexRequest) *admin_search_v1.UpdateIndexRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
		// },
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
				return
			}

			assertRpcStatus(s.T(), err, t.expect)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
			})
		})
	}
}

func (s *GatewayOpsTestSuite) TestDeleteIndex() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	indexName := newIndexName()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	s.T().Cleanup(func() {
		defer cancel()
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

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
			description: "BucketNotFound",
			modifyDefault: func(def *admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest {
				def.BucketName = ptr.To("missing-bucket")
				return def
			},
			resourceDetails: "bucket",
			expect:          codes.NotFound,
		},
		// TODO - ING-1165
		// {
		// 	description: "ScopeNamedWithoutBucket",
		// 	modifyDefault: func(def *admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest {
		// 		def.ScopeName = ptr.To("_defualt")
		// 		return def
		// 	},
		// 	resourceDetails: "scope",
		// 	expect:          codes.InvalidArgument,
		// },
		// TODO - ING-1166
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.DeleteIndexRequest) *admin_search_v1.DeleteIndexRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
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
}

func (s *GatewayOpsTestSuite) TestPauseIndexIngest() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	indexName := newIndexName()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	s.T().Cleanup(func() {
		defer cancel()
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

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
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
				def.BucketName = ptr.To("missing-bucket")
				return def
			},
			resourceDetails: "bucket",
			expect:          codes.NotFound,
		},
		{
			description: "RequestMissingBucket",
			modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
				def.BucketName = nil
				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1167
		// {
		// 	description: "ScopeNotFound",
		// 	modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
		// 		def.ScopeName = ptr.To("missing-scope")
		// 		return def
		// 	},
		// 	resourceDetails: "scope",
		// 	expect:          codes.NotFound,
		// },
		{
			description: "RequestMissingScope",
			modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
				def.ScopeName = nil
				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1168
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
		// },
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
}

func (s *GatewayOpsTestSuite) TestResumeIndexIngest() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}

	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	indexName := newIndexName()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	s.T().Cleanup(func() {
		defer cancel()
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: ptr.To("couchbase"),
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	_, err = searchAdminClient.PauseIndexIngest(ctx, &admin_search_v1.PauseIndexIngestRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))

	type pauseTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest
		expect          codes.Code
		resourceDetails string
		creds           *credentials.PerRPCCredentials
	}

	pauseTests := []pauseTest{
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
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest {
				def.BucketName = ptr.To("missing-bucket")
				return def
			},
			resourceDetails: "bucket",
			expect:          codes.NotFound,
		},
		{
			description: "RequestMissingBucket",
			modifyDefault: func(def *admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest {
				def.BucketName = nil
				return def
			},
			expect: codes.OK,
		},
		{
			description: "RequestMissingScope",
			modifyDefault: func(def *admin_search_v1.ResumeIndexIngestRequest) *admin_search_v1.ResumeIndexIngestRequest {
				def.ScopeName = nil
				return def
			},
			expect: codes.OK,
		},
		// TODO - ING-1168
		// {
		// 	description: "BadCredentials",
		// 	modifyDefault: func(def *admin_search_v1.PauseIndexIngestRequest) *admin_search_v1.PauseIndexIngestRequest {
		// 		return def
		// 	},
		// 	creds:  &s.badRpcCreds,
		// 	expect: codes.Unauthenticated,
		// },
	}

	for i := range pauseTests {
		t := pauseTests[i]
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
}

func (s *GatewayOpsTestSuite) TestIndexesQueryControl() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}
	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newIndexName()

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	sourceType := "couchbase"
	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: &sourceType,
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)
	s.T().Cleanup(func() {
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	disallowResp, err := searchAdminClient.DisallowIndexQuerying(ctx, &admin_search_v1.DisallowIndexQueryingRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), disallowResp, err)

	allowResp, err := searchAdminClient.AllowIndexQuerying(ctx, &admin_search_v1.AllowIndexQueryingRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), allowResp, err)
}

func (s *GatewayOpsTestSuite) TestIndexesPartitionControl() {
	if !s.SupportsFeature(TestFeatureSearchManagement) {
		s.T().Skip()
	}
	searchAdminClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	indexName := newIndexName()

	var bucket, scope *string
	if s.scopeName != "" && s.scopeName != "_default" {
		if !s.SupportsFeature(TestFeatureSearchManagementCollections) {
			s.T().Skip()
		}
		bucket = &s.bucketName
		scope = &s.scopeName
	}

	sourceType := "couchbase"
	resp, err := searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: &sourceType,
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)
	s.T().Cleanup(func() {
		_, _ = searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
			Name:       indexName,
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	freezeResp, err := searchAdminClient.FreezeIndexPlan(ctx, &admin_search_v1.FreezeIndexPlanRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), freezeResp, err)

	unfreezeResp, err := searchAdminClient.UnfreezeIndexPlan(ctx, &admin_search_v1.UnfreezeIndexPlanRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), unfreezeResp, err)
}
