/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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

		type createTest struct {
			description     string
			modifyDefault   func(*admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest
			expect          codes.Code
			resourceDetails string
		}

		createTests := []createTest{
			{
				description: "Create",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					return def
				},
			},
			{
				description: "CreateAlreadyExists",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					return def
				},
				expect:          codes.AlreadyExists,
				resourceDetails: "queryindex",
			},
			{
				description: "CreateAlreadyExistsIgnoreIfExists",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.IgnoreIfExists = &trueBool
					return def
				},
			},
			{
				description: "CreateBlankBucket",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.BucketName = ""
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateBlankScope",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.ScopeName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateBlankCollection",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.CollectionName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateRequestMissingScope",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.Name = &missingScopeName
					def.ScopeName = nil
					return def
				},
			},
			{
				description: "CreateRequestMissingCollection",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.Name = &missingCollectionName
					def.CollectionName = nil
					return def
				},
			},
			{
				description: "CreateRequestMissingScopeAndCollection",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.Name = &missingScopeAndCollectionName
					def.ScopeName = nil
					def.CollectionName = nil
					return def
				},
			},
			{
				description: "CreateNonExistentBucket",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.BucketName = unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "CreateNonExistentScope",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.ScopeName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "CreateNonExistentCollection",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.CollectionName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "CreateNegativeReplicas",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.NumReplicas = &negativeReplicas
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateTooManyReplicas",
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
				description: "CreateIndexNameTooLong",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					name := s.docIdOfLen(220)
					def.Name = &name
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateSpecialCharactersInIndexName",
				modifyDefault: func(def *admin_query_v1.CreatePrimaryIndexRequest) *admin_query_v1.CreatePrimaryIndexRequest {
					def.Name = &specialCharName
					return def
				},
				expect: codes.InvalidArgument,
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
				resp, err := queryAdminClient.CreatePrimaryIndex(context.Background(),
					req,
					grpc.PerRPCCredentials(s.basicRpcCreds))

				if t.expect == codes.OK {
					requireRpcSuccess(s.T(), resp, err)
					return
				}

				assertRpcStatus(s.T(), err, t.expect)
				if t.resourceDetails != "" {
					assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
						assert.Equal(s.T(), d.ResourceType, t.resourceDetails)
					})
				}
			})
		}

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
		s.Run("ListCollectionIndexesRequestMissingScope", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName:     &s.bucketName,
				CollectionName: &s.collectionName,
			})
		})

		checkNoHasIndex := func(req *admin_query_v1.GetAllIndexesRequest) {
			resp, err := queryAdminClient.GetAllIndexes(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			foundIdx := findIndex(resp.Indexes, s.bucketName, s.scopeName, s.collectionName, indexName)
			require.Nil(s.T(), foundIdx)
		}

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
		s.Run("NoListRequestMissingBucket", func() {
			checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			})
		})

		type dropTest struct {
			description       string
			defaultDifference func(*admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest
			expect            codes.Code
			resourceDetails   string
		}

		dropTests := []dropTest{
			{
				description: "DropNonExistentBucket",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.BucketName = unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "DropNonExistentScope",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.ScopeName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "DropNonExistentCollection",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.CollectionName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "Drop",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					return def
				},
			},
			{
				description: "DropMissing",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					return def
				},
				expect:          codes.NotFound,
				resourceDetails: "queryindex",
			},
			{
				description: "DropMissingIgnored",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.IgnoreIfMissing = &trueBool
					return def
				},
			},
			{
				description: "DropRequestBlankBucket",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.BucketName = ""
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "DropRequestBlankScope",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.ScopeName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "DropRequestBlankCollection",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.CollectionName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "DropRequestMissingScope",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.Name = &missingScopeName
					def.ScopeName = nil
					return def
				},
			},
			{
				description: "DropRequestMissingScopeIgnoreIfMissing",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.ScopeName = nil
					def.IgnoreIfMissing = &trueBool
					return def
				},
			},
			{
				description: "DropRequestMissingCollection",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.Name = &missingCollectionName
					def.CollectionName = nil
					return def
				},
			},
			{
				description: "DropRequestMissingCollectionIgnoreIfMissing",
				defaultDifference: func(def *admin_query_v1.DropPrimaryIndexRequest) *admin_query_v1.DropPrimaryIndexRequest {
					def.CollectionName = nil
					def.IgnoreIfMissing = &trueBool
					return def
				},
			},
			{
				description: "DropRequestMissingScopeAndCollection",
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
				resp, err := queryAdminClient.DropPrimaryIndex(context.Background(),
					req,
					grpc.PerRPCCredentials(s.basicRpcCreds))

				if t.expect == codes.OK {
					requireRpcSuccess(s.T(), resp, err)
					return
				}
				assertRpcStatus(s.T(), err, t.expect)

				if t.resourceDetails != "" {
					assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
						assert.Equal(s.T(), d.ResourceType, t.resourceDetails)
					})
				}
			})
		}
	})

	s.Run("SecondaryIndex", func() {
		indexName := uuid.NewString()

		type createTest struct {
			description     string
			modifyDefault   func(*admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest
			expect          codes.Code
			resourceDetails string
		}

		createTests := []createTest{
			{
				description: "Create",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					return def
				},
			},
			{
				description: "CreateAlreadyExists",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					return def
				},
				expect:          codes.AlreadyExists,
				resourceDetails: "queryindex",
			},
			{
				description: "CreateAlreadyExistsIgnoreIfExists",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.IgnoreIfExists = &trueBool
					return def
				},
			},
			{
				description: "CreateBlankBucket",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.BucketName = blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateBlankScope",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.ScopeName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateBlankCollection",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.CollectionName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateMissingScope",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.Name = missingScopeName
					def.ScopeName = nil
					return def
				},
			},
			{
				description: "CreateMissingCollection",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.Name = missingCollectionName
					def.CollectionName = nil
					return def
				},
			},
			{
				description: "CreateMissingScopeAndCollection",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.Name = missingScopeAndCollectionName
					def.ScopeName = nil
					def.CollectionName = nil
					return def
				},
			},
			{
				description: "CreateNonExistentBucket",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.BucketName = unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "CreateNonExistentScope",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.ScopeName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "CreateNonExistentCollection",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.CollectionName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "CreateNegativeReplicas",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					def.NumReplicas = &negativeReplicas
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateTooManyReplicas",
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
				description: "CreateIndexNameTooLong",
				modifyDefault: func(def *admin_query_v1.CreateIndexRequest) *admin_query_v1.CreateIndexRequest {
					name := s.docIdOfLen(220)
					def.Name = name
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "CreateSpecialCharactersInIndexName",
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
				resp, err := queryAdminClient.CreateIndex(context.Background(),
					req,
					grpc.PerRPCCredentials(s.basicRpcCreds))

				if t.expect == codes.OK {
					requireRpcSuccess(s.T(), resp, err)
					return
				}

				assertRpcStatus(s.T(), err, t.expect)
				if t.resourceDetails != "" {
					assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
						assert.Equal(s.T(), d.ResourceType, t.resourceDetails)
					})
				}
			})
		}

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
		s.Run("ListCollectionIndexesRequestMissingScope", func() {
			checkHasIndex(&admin_query_v1.GetAllIndexesRequest{
				BucketName:     &s.bucketName,
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
		s.Run("NoListRequestMissingBucket", func() {
			checkNoHasIndex(&admin_query_v1.GetAllIndexesRequest{
				ScopeName:      &s.scopeName,
				CollectionName: &s.collectionName,
			})
		})

		type dropTest struct {
			description     string
			modifyDefault   func(*admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest
			expect          codes.Code
			resourceDetails string
		}

		dropTests := []dropTest{
			{
				description: "DropNonExistentBucket",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.BucketName = unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "DropNonExistentScope",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.ScopeName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "DropNonExistentCollection",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.CollectionName = &unmatchedName
					return def
				},
				expect: codes.NotFound,
			},
			{
				description: "Drop",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					return def
				},
			},
			{
				description: "DropMissing",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					return def
				},
				expect:          codes.NotFound,
				resourceDetails: "queryindex",
			},
			{
				description: "DropMissingIgnored",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.IgnoreIfMissing = &trueBool
					return def
				},
			},
			{
				description: "DropRequestBlankBucket",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.BucketName = blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "DropRequestBlankScope",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.ScopeName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "DropRequestBlankCollection",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.CollectionName = &blankName
					return def
				},
				expect: codes.InvalidArgument,
			},
			{
				description: "DropRequestMissingScope",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.Name = missingScopeName
					def.ScopeName = nil
					return def
				},
			},
			{
				description: "DropRequestMissingScopeIgnoreIfMissing",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.ScopeName = nil
					def.IgnoreIfMissing = &trueBool
					return def
				},
			},
			{
				description: "DropRequestMissingCollection",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.Name = missingCollectionName
					def.CollectionName = nil
					return def
				},
			},
			{
				description: "DropRequestMissingCollectionIgnoreIfMissing",
				modifyDefault: func(def *admin_query_v1.DropIndexRequest) *admin_query_v1.DropIndexRequest {
					def.Name = missingCollectionName
					def.CollectionName = nil
					def.IgnoreIfMissing = &trueBool
					return def
				},
			},
			{
				description: "DropRequestMissingScopeAndCollection",
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
				resp, err := queryAdminClient.DropIndex(context.Background(),
					req,
					grpc.PerRPCCredentials(s.basicRpcCreds))

				if t.expect == codes.OK {
					requireRpcSuccess(s.T(), resp, err)
					return
				}
				assertRpcStatus(s.T(), err, t.expect)

				if t.resourceDetails != "" {
					assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
						assert.Equal(s.T(), d.ResourceType, t.resourceDetails)
					})
				}
			})
		}
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
