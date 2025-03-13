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
	"encoding/json"
	"time"

	"github.com/stretchr/testify/assert"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func newIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
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
