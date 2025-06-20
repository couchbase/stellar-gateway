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

func (s *GatewayOpsTestSuite) TestSearchMgmtGet() {
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

	type getTest struct {
		description     string
		modifyDefault   func(*admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest
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
		// 	expect: codes.InvalidArgument,
		// },
		{
			description: "IndexNotFound",
			modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
				def.Name = "missing-index"
				return def
			},
			expect: codes.NotFound,
		},
		{
			description: "EmptyBucketName",
			modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
				def.BucketName = nil
				return def
			},
			expect: codes.OK,
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
			expect: codes.NotFound,
		},
		// TODO -ING-1152
		// {
		// 	description: "ScopeNotFound",
		// 	modifyDefault: func(def *admin_search_v1.GetIndexRequest) *admin_search_v1.GetIndexRequest {
		// 		def.ScopeName = ptr.To("missing-scope")
		// 		return def
		// 	},
		// 	expect: codes.NotFound,
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
		})
	}
}

func (s *GatewayOpsTestSuite) TestSearchMgmtCreate() {
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
		description   string
		modifyDefault func(*admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest
		expect        codes.Code
		creds         *credentials.PerRPCCredentials
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
			expect: codes.NotFound,
		},
		// TODO - ING-1148
		// {
		// 	description: "ScopeNotFound",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = newIndexName()
		// 		def.ScopeName = ptr.To("missing-scope")
		// 		return def
		// 	},
		// 	expect: codes.NotFound,
		// },
		// TODO - ING-1144
		// {
		// 	description: "SpecialCharInIndexName",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = "special-char-name!"
		// 		return def
		// 	},
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1145
		// {
		// 	description: "IndexNameTooLong",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Name = s.docIdOfLen(260)
		// 		return def
		// 	},
		// 	expect: codes.InvalidArgument,
		// },
		// TODO - ING-1147
		// {
		// 	description: "InvalidSourceType",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.SourceType = ptr.To("not-a-source-type")
		// 		return def
		// 	},
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
		// 	expect: codes.NotFound || codes.InvalidArgument,
		// },
		// TODO - ING-1150
		// {
		// 	description: "InvalidType",
		// 	modifyDefault: func(def *admin_search_v1.CreateIndexRequest) *admin_search_v1.CreateIndexRequest {
		// 		def.Type = "not-a-type"
		// 		return def
		// 	},
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
		})
	}
}

func (s *GatewayOpsTestSuite) TestCreateUpdateGetDeleteIndex() {
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

	index, err := searchAdminClient.GetIndex(ctx, &admin_search_v1.GetIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), index, err)

	s.Assert().Equal(indexName, index.Index.Name)
	s.Assert().Equal("fulltext-index", index.Index.Type)

	indexes, err := searchAdminClient.ListIndexes(ctx, &admin_search_v1.ListIndexesRequest{
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), indexes, err)

	var found bool
	for _, i := range indexes.Indexes {
		if i.Name == indexName {
			found = true
			break
		}
	}
	s.Assert().True(found, "Did not find expected index in GetAllIndexes")

	updateIndex := index.Index
	planParams := map[string]interface{}{
		"test":  map[string]interface{}{},
		"test2": map[string]interface{}{},
	}
	b, err := json.Marshal(planParams)
	s.Require().NoError(err)

	updateIndex.PlanParams = map[string][]byte{
		"targets": b,
	}
	updateResp, err := searchAdminClient.UpdateIndex(ctx, &admin_search_v1.UpdateIndexRequest{
		Index:      updateIndex,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), updateResp, err)

	delResp, err := searchAdminClient.DeleteIndex(ctx, &admin_search_v1.DeleteIndexRequest{
		Name:       updateIndex.Name,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), delResp, err)
}

func (s *GatewayOpsTestSuite) TestIndexesIngestControl() {
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

	pauseResp, err := searchAdminClient.PauseIndexIngest(ctx, &admin_search_v1.PauseIndexIngestRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), pauseResp, err)

	resumeResp, err := searchAdminClient.ResumeIndexIngest(ctx, &admin_search_v1.ResumeIndexIngestRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resumeResp, err)
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

func (s *GatewayOpsTestSuite) TestCreateIndexAlreadyExists() {
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

	_, err = searchAdminClient.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		BucketName: bucket,
		ScopeName:  scope,
		Type:       "fulltext-index",
		SourceType: &sourceType,
		SourceName: &s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.AlreadyExists)
	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
		assert.Equal(s.T(), d.ResourceType, "searchindex")
	})
}

func (s *GatewayOpsTestSuite) TestUpdateIndexUUIDMismatch() {
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

	_, err = searchAdminClient.UpdateIndex(ctx, &admin_search_v1.UpdateIndexRequest{
		Index: &admin_search_v1.Index{
			Name:       indexName,
			Type:       "fulltext-index",
			SourceType: &sourceType,
			SourceName: &s.bucketName,
			Uuid:       "123456",
		},
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.Aborted)
	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
		assert.Equal(s.T(), d.ResourceType, "searchindex")
	})
}
