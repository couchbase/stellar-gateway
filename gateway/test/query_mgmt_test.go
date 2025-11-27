package test

import (
	"context"
	"fmt"

	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

func (s *GatewayOpsTestSuite) TestQueryManagement() {
	if !s.SupportsFeature(TestFeatureQueryManagement) {
		s.T().Skip()
	}
	queryAdminClient := admin_query_v1.NewQueryAdminServiceClient(s.gatewayConn)

	trueBool := true
	unmatchedName := "something-else"
	specialCharName := "index!"
	missingScopeName := "scopeMissingIndex"
	missingCollectionName := "collectionMissingIndex"
	missingScopeAndCollectionName := "scopeAndCollectionMissingIndex"
	blankName := ""
	negativeReplicas := int32(-1)
	tooManyReplicas := int32(10)

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
			type createTest struct {
				description     string
				modifyDefault   func(*admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest
				getCreds        func() credentials.PerRPCCredentials
				expect          codes.Code
				resourceDetails string
			}

			createTests := []createTest{
				{
					description: "Basic",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						return def
					},
				},
				{
					description: "AlreadyExists",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						return def
					},
					expect:          codes.AlreadyExists,
					resourceDetails: "queryindex",
				},
				{
					description: "AlreadyExistsIgnoreIfExists",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.IgnoreIfExists = &trueBool
						return def
					},
				},
				{
					description: "BlankBucket",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.BucketName = ""
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankScope",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.ScopeName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankCollection",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.CollectionName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "RequestMissingScope",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.Name = &missingScopeName
						def.ScopeName = nil
						return def
					},
				},
				{
					description: "RequestMissingCollection",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.Name = &missingCollectionName
						def.CollectionName = nil
						return def
					},
				},
				{
					description: "RequestMissingScopeAndCollection",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.Name = &missingScopeAndCollectionName
						def.ScopeName = nil
						def.CollectionName = nil
						return def
					},
				},
				{
					description: "NonExistentBucket",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.BucketName = unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentScope",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.ScopeName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentCollection",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.CollectionName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NegativeReplicas",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.NumReplicas = &negativeReplicas
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "TooManyReplicas",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						// Index name collision detection occurs before attempting to build the index so a new index name
						// needs to be used, else we get codes.AlreadyExists
						name := "newIndex"
						def.Name = &name
						def.NumReplicas = &tooManyReplicas
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "IndexNameTooLong",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						name := s.docIdOfLen(220)
						def.Name = &name
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "SpecialCharactersInIndexName",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						def.Name = &specialCharName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BadCredentials",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						name := "newIndex"
						def.Name = &name
						return def
					},
					getCreds: s.getBadRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "NoPermissionCreds",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						name := "newIndex"
						def.Name = &name
						return def
					},
					getCreds: s.getNoPermissionRpcCreds,
					expect:   codes.PermissionDenied,
				},
				{
					description: "ReadOnlyCreds",
					modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
						name := "newIndex"
						def.Name = &name
						return def
					},
					getCreds: s.getReadOnlyRpcCredentials,
					expect:   codes.PermissionDenied,
				},
			}

			for i := range createTests {
				t := createTests[i]
				s.Run(t.description, func() {
					defaultCreateReq := admin_query_v1.CreatePrimaryIndexRequest{
						Name:           &indexName,
						BucketName:     s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					}
					req := t.modifyDefault(&defaultCreateReq)

					creds := s.basicRpcCreds
					if t.getCreds != nil {
						creds = t.getCreds()
					}

					resp, err := queryAdminClient.CreatePrimaryIndex(context.Background(),
						req,
						grpc.PerRPCCredentials(creds))

					if t.expect == codes.OK {
						requireRpcSuccess(s.T(), resp, err)
						return
					}

					assertRpcStatus(s.T(), err, t.expect)
					if t.resourceDetails != "" {
						assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
							assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
						})
					}
				})
			}
		})

		s.Run("ListAllIndexes", func() {
			type listTest struct {
				description   string
				req           *admin_query_v1.GetAllIndexesRequest
				getCreds      func() credentials.PerRPCCredentials
				expect        codes.Code
				expectNoIndex bool
			}

			listTests := []listTest{
				{
					description: "Basic",
					req:         &admin_query_v1.GetAllIndexesRequest{},
				},
				{
					description: "BucketIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName: &s.bucketName,
					},
				},
				{
					description: "ScopeIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName: &s.bucketName,
						ScopeName:  &s.scopeName,
					},
				},
				{
					description: "CollectionIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName:     &s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					},
				},
				{
					description: "CollectionIndexesMissingScope",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName:     &s.bucketName,
						CollectionName: &s.collectionName,
					},
				},
				{
					description: "BadCredentials",
					req:         &admin_query_v1.GetAllIndexesRequest{},
					getCreds:    s.getBadRpcCredentials,
					expect: func() codes.Code {
						// The query we do to get all indexes returns a 200
						// 200 status against any version pre 7.6.x
						if s.IsOlderServerVersion("7.6") {
							return codes.OK
						}
						return codes.PermissionDenied
					}(),
					expectNoIndex: true,
				},
				{
					description:   "NoPermissionCreds",
					req:           &admin_query_v1.GetAllIndexesRequest{},
					getCreds:      s.getNoPermissionRpcCreds,
					expectNoIndex: true,
				},
				// BUG(ING-506): Can't test with invalid bucket names
				// {
				// 	description: "NoListBucketIndexes",
				// 	req: &admin_query_v1.GetAllIndexesRequest{
				// 		BucketName: &unmatchedName,
				// 	},
				// 	expectNoIndex: true,
				// },
				{
					description: "NoListScopeIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName: &s.bucketName,
						ScopeName:  &unmatchedName,
					},
					expectNoIndex: true,
				},
				{
					description: "NoListCollectionIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName:     &s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &unmatchedName,
					},
					expectNoIndex: true,
				},
				{
					description: "NoListRequestMissingBucket",
					req: &admin_query_v1.GetAllIndexesRequest{
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					},
					expectNoIndex: true,
				},
			}

			for i := range listTests {
				t := listTests[i]
				s.Run(t.description, func() {
					creds := s.basicRpcCreds
					if t.getCreds != nil {
						creds = t.getCreds()
					}

					resp, err := queryAdminClient.GetAllIndexes(context.Background(), t.req, grpc.PerRPCCredentials(creds))
					if t.expect == codes.OK {
						requireRpcSuccess(s.T(), resp, err)
						foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)

						if t.expectNoIndex {
							require.Nil(s.T(), foundIdx)
							return
						}

						require.NotNil(s.T(), foundIdx)
						return
					}

					assertRpcStatus(s.T(), err, t.expect)
				})
			}
		})

		s.Run("Drop", func() {
			type dropTest struct {
				description       string
				defaultDifference func(*admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest
				getCreds          func() credentials.PerRPCCredentials
				expect            codes.Code
				resourceDetails   string
			}

			dropTests := []dropTest{
				{
					description: "NonExistentBucket",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.BucketName = unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentScope",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.ScopeName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentCollection",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.CollectionName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "BadCredentials",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						return def
					},
					getCreds: s.getBadRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "NoPermissionCreds",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						return def
					},
					getCreds: s.getNoPermissionRpcCreds,
					expect:   codes.PermissionDenied,
				},
				{
					description: "ReadOnlyCreds",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						return def
					},
					getCreds: s.getReadOnlyRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "Basic",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						return def
					},
				},
				{
					description: "IndexMissing",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						return def
					},
					expect:          codes.NotFound,
					resourceDetails: "queryindex",
				},
				{
					description: "IndexMissingIgnored",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.IgnoreIfMissing = &trueBool
						return def
					},
				},
				{
					description: "BlankBucket",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.BucketName = ""
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankScope",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.ScopeName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankCollection",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.CollectionName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "MissingScope",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.Name = &missingScopeName
						def.ScopeName = nil
						return def
					},
				},
				{
					description: "MissingScopeIgnoreIfMissing",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.ScopeName = nil
						def.IgnoreIfMissing = &trueBool
						return def
					},
				},
				{
					description: "MissingCollection",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.Name = &missingCollectionName
						def.CollectionName = nil
						return def
					},
				},
				{
					description: "MissingCollectionIgnoreIfMissing",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.CollectionName = nil
						def.IgnoreIfMissing = &trueBool
						return def
					},
				},
				{
					description: "MissingScopeAndCollection",
					defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
						def.Name = &missingScopeAndCollectionName
						def.CollectionName = nil
						return def
					},
				},
			}

			for i := range dropTests {
				t := dropTests[i]
				s.Run(t.description, func() {
					defaultDropRequest := admin_query_v1.DropPrimaryIndexRequest{
						Name:           &indexName,
						BucketName:     s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					}
					req := t.defaultDifference(&defaultDropRequest)

					creds := s.basicRpcCreds
					if t.getCreds != nil {
						creds = t.getCreds()
					}

					resp, err := queryAdminClient.DropPrimaryIndex(context.Background(),
						req,
						grpc.PerRPCCredentials(creds))

					if t.expect == codes.OK {
						requireRpcSuccess(s.T(), resp, err)
						return
					}
					assertRpcStatus(s.T(), err, t.expect)

					if t.resourceDetails != "" {
						assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
							assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
						})
					}
				})
			}
		})
	})

	s.Run("SecondaryIndex", func() {
		indexName := uuid.NewString()

		s.Run("Create", func() {
			type createTest struct {
				description     string
				modifyDefault   func(*admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest
				getCreds        func() credentials.PerRPCCredentials
				expect          codes.Code
				resourceDetails string
			}

			createTests := []createTest{
				{
					description: "Basic",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						return def
					},
				},
				{
					description: "BadCredentials",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						return def
					},
					getCreds: s.getBadRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "NoPermissionCreds",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						return def
					},
					getCreds: s.getNoPermissionRpcCreds,
					expect:   codes.PermissionDenied,
				},
				{
					description: "ReadOnlyCreds",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						return def
					},
					getCreds: s.getReadOnlyRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "AlreadyExists",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						return def
					},
					expect:          codes.AlreadyExists,
					resourceDetails: "queryindex",
				},
				{
					description: "AlreadyExistsIgnoreIfExists",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.IgnoreIfExists = &trueBool
						return def
					},
				},
				{
					description: "BlankBucket",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.BucketName = blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankScope",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.ScopeName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankCollection",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.CollectionName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "MissingScope",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.Name = missingScopeName
						def.ScopeName = nil
						return def
					},
				},
				{
					description: "MissingCollection",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.Name = missingCollectionName
						def.CollectionName = nil
						return def
					},
				},
				{
					description: "MissingScopeAndCollection",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.Name = missingScopeAndCollectionName
						def.ScopeName = nil
						def.CollectionName = nil
						return def
					},
				},
				{
					description: "NonExistentBucket",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.BucketName = unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentScope",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.ScopeName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentCollection",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.CollectionName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NegativeReplicas",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.NumReplicas = &negativeReplicas
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "TooManyReplicas",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						// Index name collision detection occurs before attempting to build the index so a new index name
						// needs to be used, else we get codes.AlreadyExists
						def.Name = "newIndex"
						def.NumReplicas = &tooManyReplicas
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "IndexNameTooLong",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						name := s.docIdOfLen(220)
						def.Name = name
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "SpecialCharactersInIndexName",
					modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
						def.Name = specialCharName
						return def
					},
					expect: codes.InvalidArgument,
				},
			}

			for i := range createTests {
				t := createTests[i]
				s.Run(t.description, func() {
					defaultCreateReq := admin_query_v1.CreateIndexRequest{
						Name:           indexName,
						BucketName:     s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
						Fields:         []string{"test"},
					}
					req := t.modifyDefault(&defaultCreateReq)

					creds := s.basicRpcCreds
					if t.getCreds != nil {
						creds = t.getCreds()
					}

					resp, err := queryAdminClient.CreateIndex(context.Background(),
						req,
						grpc.PerRPCCredentials(creds))

					if t.expect == codes.OK {
						requireRpcSuccess(s.T(), resp, err)
						return
					}

					assertRpcStatus(s.T(), err, t.expect)
					if t.resourceDetails != "" {
						assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
							assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
						})
					}
				})
			}
		})

		s.Run("ListAllIndexes", func() {
			type listTest struct {
				description   string
				req           *admin_query_v1.GetAllIndexesRequest
				creds         *credentials.PerRPCCredentials
				expect        codes.Code
				expectNoIndex bool
			}

			listTests := []listTest{
				{
					description: "Basic",
					req:         &admin_query_v1.GetAllIndexesRequest{},
				},
				{
					description: "BucketIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName: &s.bucketName,
					},
				},
				{
					description: "ScopeIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName: &s.bucketName,
						ScopeName:  &s.scopeName,
					},
				},
				{
					description: "CollectionIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName:     &s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					},
				},
				{
					description: "CollectionIndexesMissingScope",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName:     &s.bucketName,
						CollectionName: &s.collectionName,
					},
				},
				// BUG(ING-506): Can't test with invalid bucket names
				// {
				// 	description: "NoListBucketIndexes",
				// 	req: &admin_query_v1.GetAllIndexesRequest{
				// 		BucketName: &unmatchedName,
				// 	},
				// 	expectNoIndex: true,
				// },
				{
					description: "NoListScopeIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName: &s.bucketName,
						ScopeName:  &unmatchedName,
					},
					expectNoIndex: true,
				},
				{
					description: "NoListCollectionIndexes",
					req: &admin_query_v1.GetAllIndexesRequest{
						BucketName:     &s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &unmatchedName,
					},
					expectNoIndex: true,
				},
				{
					description: "NoListRequestMissingBucket",
					req: &admin_query_v1.GetAllIndexesRequest{
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					},
					expectNoIndex: true,
				},
			}

			for i := range listTests {
				t := listTests[i]
				s.Run(t.description, func() {
					creds := s.basicRpcCreds
					if t.creds != nil {
						creds = *t.creds
					}

					resp, err := queryAdminClient.GetAllIndexes(context.Background(), t.req, grpc.PerRPCCredentials(creds))
					if t.expect == codes.OK {
						requireRpcSuccess(s.T(), resp, err)
						foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)

						if t.expectNoIndex {
							require.Nil(s.T(), foundIdx)
							return
						}

						require.NotNil(s.T(), foundIdx)
						return
					}

					assertRpcStatus(s.T(), err, t.expect)
				})
			}
		})

		s.Run("Drop", func() {
			type dropTest struct {
				description     string
				modifyDefault   func(*admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest
				getCreds        func() credentials.PerRPCCredentials
				expect          codes.Code
				resourceDetails string
			}

			dropTests := []dropTest{
				{
					description: "NonExistentBucket",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.BucketName = unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentScope",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.ScopeName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "NonExistentCollection",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.CollectionName = &unmatchedName
						return def
					},
					expect: codes.NotFound,
				},
				{
					description: "Basic",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						return def
					},
				},
				{
					description: "BadCredentials",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						return def
					},
					getCreds: s.getBadRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "NoPermissionCreds",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						return def
					},
					getCreds: s.getNoPermissionRpcCreds,
					expect:   codes.PermissionDenied,
				},
				{
					description: "ReadOnlyCreds",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						return def
					},
					getCreds: s.getReadOnlyRpcCredentials,
					expect:   codes.PermissionDenied,
				},
				{
					description: "IndexMissing",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						return def
					},
					expect:          codes.NotFound,
					resourceDetails: "queryindex",
				},
				{
					description: "IndexMissingIgnored",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.IgnoreIfMissing = &trueBool
						return def
					},
				},
				{
					description: "BlankBucket",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.BucketName = blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankScope",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.ScopeName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "BlankCollection",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.CollectionName = &blankName
						return def
					},
					expect: codes.InvalidArgument,
				},
				{
					description: "MissingScope",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.Name = missingScopeName
						def.ScopeName = nil
						return def
					},
				},
				{
					description: "MissingScopeIgnoreIfMissing",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.ScopeName = nil
						def.IgnoreIfMissing = &trueBool
						return def
					},
				},
				{
					description: "MissingCollection",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.Name = missingCollectionName
						def.CollectionName = nil
						return def
					},
				},
				{
					description: "MissingCollectionIgnoreIfMissing",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.Name = missingCollectionName
						def.CollectionName = nil
						def.IgnoreIfMissing = &trueBool
						return def
					},
				},
				{
					description: "MissingScopeAndCollection",
					modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
						def.Name = missingScopeAndCollectionName
						def.CollectionName = nil
						return def
					},
				},
			}

			for i := range dropTests {
				t := dropTests[i]
				s.Run(t.description, func() {
					defaultDropRequest := admin_query_v1.DropIndexRequest{
						Name:           indexName,
						BucketName:     s.bucketName,
						ScopeName:      &s.scopeName,
						CollectionName: &s.collectionName,
					}
					req := t.modifyDefault(&defaultDropRequest)

					creds := s.basicRpcCreds
					if t.getCreds != nil {
						creds = t.getCreds()
					}

					resp, err := queryAdminClient.DropIndex(context.Background(),
						req,
						grpc.PerRPCCredentials(creds))

					if t.expect == codes.OK {
						requireRpcSuccess(s.T(), resp, err)
						return
					}
					assertRpcStatus(s.T(), err, t.expect)

					if t.resourceDetails != "" {
						assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
							assert.Equal(s.T(), t.resourceDetails, d.ResourceType)
						})
					}
				})
			}
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

		s.Run("BuildBadCreds", func() {
			resp, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.badRpcCreds))

			// When we build deferred indexes we first GetAllIndexes. When done
			// with bad credentials pre 7.6.x GetAllIndexes returns an OK status
			// and an empty list of indexes, causing buildDeferredIndexes to do
			// the same.
			if s.IsOlderServerVersion("7.6") {
				assertRpcStatus(s.T(), err, codes.OK)
				assert.Len(s.T(), resp.Indexes, 0)
				return
			}

			assertRpcStatus(s.T(), err, codes.PermissionDenied)
		})

		// Building with no perissions does not return an error status but does
		// not build the indexes
		s.Run("BuildNoPermissionCreds", func() {
			resp, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.getNoPermissionRpcCreds()))
			requireRpcSuccess(s.T(), resp, err)

			getResp, err := queryAdminClient.GetAllIndexes(context.Background(), &admin_query_v1.GetAllIndexesRequest{}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			foundIdx := findIndex(getResp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.NotNil(s.T(), foundIdx)

			require.Equal(s.T(), admin_query_v1.IndexState_INDEX_STATE_DEFERRED, foundIdx.State)
		})

		s.Run("BuildReadOnlyCreds", func() {
			_, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
				BucketName:     s.bucketName,
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			}, grpc.PerRPCCredentials(s.getReadOnlyRpcCredentials()))
			assertRpcStatus(s.T(), err, codes.PermissionDenied)
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
	s.T().Cleanup(deleteScope1)

	deleteScope2 := s.CreateScope(s.bucketName, scope2)
	s.T().Cleanup(deleteScope2)

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

	// Specifying a bucket that does not exist should return not found
	buildResp, err = queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName: "not-a-real-bucket",
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.NotFound)
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
	s.T().Cleanup(deleteScope1)

	deleteScope2 := s.CreateScope(s.bucketName, scope2)
	s.T().Cleanup(deleteScope2)

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

	// Specifying a scope or collection name that contains spaces should return invalid argument
	spaceName := "name with spaces"
	buildResp, err = queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName:     s.bucketName,
		ScopeName:      &spaceName,
		CollectionName: &collection,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.InvalidArgument)

	buildResp, err = queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName:     s.bucketName,
		ScopeName:      &scope1,
		CollectionName: &spaceName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.InvalidArgument)
}

// This test ensures that an empty collection and scope are treated as _default.
func (s *GatewayOpsTestSuite) TestQueryManagementWaitForIndexOnlineEmptyCollectionAndScope() {
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
	s.T().Cleanup(deleteScope1)

	deleteScope2 := s.CreateScope(s.bucketName, scope2)
	s.T().Cleanup(deleteScope2)

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
		fmt.Printf("%#v\n", err)
		requireRpcSuccess(s.T(), iResp, err)
	}

	createIndex(scope1, collection)
	createIndex(scope2, collection)
	createIndex(defaultScope, defaultCollection)

	buildResp, err := queryAdminClient.BuildDeferredIndexes(context.Background(), &admin_query_v1.BuildDeferredIndexesRequest{
		BucketName:     s.bucketName,
		ScopeName:      &defaultScope,
		CollectionName: &defaultCollection,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), buildResp, err)

	waitResp, err := queryAdminClient.WaitForIndexOnline(context.Background(), &admin_query_v1.WaitForIndexOnlineRequest{
		BucketName:     s.bucketName,
		ScopeName:      "",
		CollectionName: "",
		Name:           indexName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), waitResp, err)
}
