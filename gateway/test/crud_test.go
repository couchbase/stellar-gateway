package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
)

type commonErrorTestData struct {
	ScopeName      string
	BucketName     string
	CollectionName string
	CallOptions    []grpc.CallOption
}

func (s *GatewayOpsTestSuite) RunCommonErrorCases(
	fn func(opts *commonErrorTestData) (interface{}, error),
) {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}

	s.Run("CollectionMissing", func() {
		_, err := fn(&commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: "invalid-collection",
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "collection")
		})
	})

	s.Run("ScopeMissing", func() {
		_, err := fn(&commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      "invalid-scope",
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "scope")
		})
	})

	s.Run("BucketMissing", func() {
		_, err := fn(&commonErrorTestData{
			BucketName:     "invalid-bucket",
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "bucket")
		})
	})

	s.Run("BadCredentials", func() {
		_, err := fn(&commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.badRpcCreds),
			},
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "user")
		})
	})

	s.Run("Unauthenticated", func() {
		_, err := fn(&commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions:    []grpc.CallOption{},
		})
		assertRpcStatus(s.T(), err, codes.Unauthenticated)
	})
}

func (s *GatewayOpsTestSuite) TestGet() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), resp.Content, TEST_CONTENT)
		assert.Equal(s.T(), resp.ContentFlags, TEST_CONTENT_FLAGS)
		assert.Nil(s.T(), resp.Expiry)
	})

	s.Run("ProjectSimple", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Project:        []string{"obj.num", "arr"},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.JSONEq(s.T(), string(resp.Content), `{"obj":{"num":14},"arr":[3,6,9,12]}`)
		assert.Equal(s.T(), resp.ContentFlags, uint32(0))
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestInsert() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.randomDocId()
		resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.Run("DocExists", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("ExpirySecs", func() {
		s.Run("ConversionUnder30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.InsertRequest_ExpirySecs{ExpirySecs: 5},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: 5 + 1,
					MinSecs: 0,
				},
			})
		})

		s.Run("Conversion30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.InsertRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((29 * 24 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ConversionOver30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.InsertRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((30 * 24 * time.Hour).Seconds()),
				},
			})
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        s.largeTestContent(),
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestUpsert() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.randomDocId()
		resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ExpirySecs", func() {
		s.Run("ConversionUnder30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: 5},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: 5 + 1,
					MinSecs: 0,
				},
			})
		})

		s.Run("Conversion30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((29 * 24 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ConversionOver30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((30 * 24 * time.Hour).Seconds()),
				},
			})
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        s.largeTestContent(),
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestReplace() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
	newContent := []byte(`{"boo": "baz"}`)

	s.Run("Basic", func() {
		docId := s.testDocId()

		resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        newContent,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        newContent,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        newContent,
			ContentFlags:   TEST_CONTENT_FLAGS,
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)
	})

	s.Run("CasMismatch", func() {
		docId, docCas := s.testDocIdAndCas()
		incorrectCas := s.incorrectCas(docCas)

		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        newContent,
			ContentFlags:   TEST_CONTENT_FLAGS,
			Cas:            &incorrectCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), d.Reason, "CAS_MISMATCH")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ExpirySecs", func() {
		s.Run("ConversionUnder30Days", func() {
			docId := s.testDocId()
			resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.ReplaceRequest_ExpirySecs{ExpirySecs: 5},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: 5 + 1,
					MinSecs: 0,
				},
			})
		})

		s.Run("Conversion30Days", func() {
			docId := s.testDocId()
			resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.ReplaceRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((29 * 24 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ConversionOver30Days", func() {
			docId := s.testDocId()
			resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Expiry:         &kv_v1.ReplaceRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((30 * 24 * time.Hour).Seconds()),
				},
			})
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        s.largeTestContent(),
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestRemove() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.testDocId()
		resp, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        nil,
		})
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)
	})

	s.Run("CasMismatch", func() {
		docId, docCas := s.testDocIdAndCas()
		incorrectCas := s.incorrectCas(docCas)

		_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Cas:            &incorrectCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), d.Reason, "CAS_MISMATCH")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:      opts.BucketName,
			ScopeName:       opts.ScopeName,
			CollectionName:  opts.CollectionName,
			Key:             s.randomDocId(),
			Cas:             nil,
			DurabilityLevel: nil,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestTouch() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.testDocId()
		resp, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 20},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		// check that the expiry was actually set
		getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), getResp, err)
		assertValidCas(s.T(), getResp.Cas)
		expiryTime := time.Unix(getResp.Expiry.Seconds, int64(getResp.Expiry.Nanos))
		expirySecs := int(time.Until(expiryTime) / time.Second)
		assert.Greater(s.T(), expirySecs, 0)
		assert.LessOrEqual(s.T(), expirySecs, 20+1)
	})

	s.Run("Conversion30Days", func() {
		docId := s.testDocId()
		resp, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
				MinSecs: int((29 * 24 * time.Hour).Seconds()),
			},
		})
	})

	s.Run("ConversionOver30Days", func() {
		docId := s.testDocId()
		resp, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
				MinSecs: int((30 * 24 * time.Hour).Seconds()),
			},
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 10},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 10},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: 10},
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestGetAndTouch() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.testDocId()
		resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 20},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), resp.Content, TEST_CONTENT)
		assert.Equal(s.T(), resp.ContentFlags, TEST_CONTENT_FLAGS)

		// check that the expiry was actually set
		getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), getResp, err)
		assertValidCas(s.T(), getResp.Cas)
		expiryTime := time.Unix(getResp.Expiry.Seconds, int64(getResp.Expiry.Nanos))
		expirySecs := int(time.Until(expiryTime) / time.Second)
		assert.Greater(s.T(), expirySecs, 0)
		assert.LessOrEqual(s.T(), expirySecs, 20+1)
	})

	s.Run("Conversion30Days", func() {
		docId := s.testDocId()
		resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
				MinSecs: int((29 * 24 * time.Hour).Seconds()),
			},
		})
	})

	s.Run("ConversionOver30Days", func() {
		docId := s.testDocId()
		resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
				MinSecs: int((30 * 24 * time.Hour).Seconds()),
			},
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 10},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 10},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 10},
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestGetAndLock() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.testDocId()
		resp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			LockTime:       30,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), resp.Content, TEST_CONTENT)
		assert.Equal(s.T(), resp.ContentFlags, TEST_CONTENT_FLAGS)
		assert.Nil(s.T(), resp.Expiry)

		// Validate that the document is locked and we can't do updates
		_, err = kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        TEST_CONTENT,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			LockTime:       5,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			LockTime:       5,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			LockTime:       5,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestUnlock() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.testDocId()
		galResp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			LockTime:       30,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), galResp, err)

		// Attempt to unlock the document
		_, err = kvClient.Unlock(context.Background(), &kv_v1.UnlockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Cas:            galResp.Cas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), galResp, err)
	})

	s.Run("WrongCas", func() {
		galDocId := s.testDocId()
		galResp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            galDocId,
			LockTime:       30,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), galResp, err)

		// Check that unlock fails with the wrong cas
		_, err = kvClient.Unlock(context.Background(), &kv_v1.UnlockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            galDocId,
			Cas:            s.incorrectCas(galResp.Cas),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), d.Reason, "CAS_MISMATCH")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Unlock(context.Background(), &kv_v1.UnlockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Cas:            10,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Unlock(context.Background(), &kv_v1.UnlockRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Cas:            10,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestExists() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		resp, err := kvClient.Exists(context.Background(), &kv_v1.ExistsRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.True(s.T(), resp.Result)
	})

	s.Run("DocMissing", func() {
		resp, err := kvClient.Exists(context.Background(), &kv_v1.ExistsRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assert.Zero(s.T(), resp.Cas)
		assert.False(s.T(), resp.Result)
	})

	s.Run("DocLocked", func() {
		resp, err := kvClient.Exists(context.Background(), &kv_v1.ExistsRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Exists(context.Background(), &kv_v1.ExistsRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestIncrement() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("5"))

		resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(6), resp.Content)

		checkDocument(docId, []byte("6"))
	})

	s.Run("WithInitialExists", func() {
		docId := s.binaryDocId([]byte("5"))
		initialValue := int64(5)

		resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        &initialValue,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(6), resp.Content)

		checkDocument(docId, []byte("6"))
	})

	s.Run("WithInitialMissing", func() {
		docId := s.randomDocId()
		initialValue := int64(5)

		resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        &initialValue,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(5), resp.Content)

		checkDocument(docId, []byte("5"))
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Delta:          1,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		docId := s.binaryDocId([]byte("5"))
		s.lockDoc(docId)

		_, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ExpirySecs", func() {
		var initialValue int64 = 5
		s.Run("ConversionUnder30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Delta:          1,
				Initial:        &initialValue,
				Expiry:         &kv_v1.IncrementRequest_ExpirySecs{ExpirySecs: 5},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte("5"),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: 5 + 1,
					MinSecs: 0,
				},
			})
		})

		s.Run("Conversion30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Delta:          1,
				Initial:        &initialValue,
				Expiry:         &kv_v1.IncrementRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte("5"),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((29 * 24 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ConversionOver30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Delta:          1,
				Initial:        &initialValue,
				Expiry:         &kv_v1.IncrementRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte("5"),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((30 * 24 * time.Hour).Seconds()),
				},
			})
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Delta:          1,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestDecrement() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("5"))

		resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(4), resp.Content)

		checkDocument(docId, []byte("4"))
	})

	s.Run("WithInitialExists", func() {
		docId := s.binaryDocId([]byte("5"))
		initialValue := int64(5)

		resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        &initialValue,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(4), resp.Content)

		checkDocument(docId, []byte("4"))
	})

	s.Run("WithInitialMissing", func() {
		docId := s.randomDocId()
		initialValue := int64(5)

		resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        &initialValue,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(5), resp.Content)

		checkDocument(docId, []byte("5"))
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Delta:          1,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		docId := s.binaryDocId([]byte("5"))
		s.lockDoc(docId)

		_, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ExpirySecs", func() {
		var initialValue int64 = 5
		s.Run("ConversionUnder30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Delta:          1,
				Initial:        &initialValue,
				Expiry:         &kv_v1.DecrementRequest_ExpirySecs{ExpirySecs: 5},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte("5"),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: 5 + 1,
					MinSecs: 0,
				},
			})
		})

		s.Run("Conversion30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Delta:          1,
				Initial:        &initialValue,
				Expiry:         &kv_v1.DecrementRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte("5"),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((29 * 24 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ConversionOver30Days", func() {
			docId := s.randomDocId()
			resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Delta:          1,
				Initial:        &initialValue,
				Expiry:         &kv_v1.DecrementRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte("5"),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((30 * 24 * time.Hour).Seconds()),
				},
			})
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Delta:          1,
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestAppend() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("hello"))

		resp, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("helloworld"),
			ContentFlags:   0,
		})
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)
	})

	s.Run("CasMismatch", func() {
		docId, docCas := s.testDocIdAndCas()
		incorrectCas := s.incorrectCas(docCas)

		_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
			Cas:            &incorrectCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), d.Reason, "CAS_MISMATCH")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Content:        []byte("world"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        []byte("world"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Content:        s.largeTestContent(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), "VALUE_TOO_LARGE", d.Violations[0].Type)
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Content:        []byte("world"),
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestPrepend() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("hello"))

		resp, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("worldhello"),
			ContentFlags:   0,
		})
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)
	})

	s.Run("CasMismatch", func() {
		docId, docCas := s.testDocIdAndCas()
		incorrectCas := s.incorrectCas(docCas)

		_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
			Cas:            &incorrectCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), d.Reason, "CAS_MISMATCH")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Content:        []byte("world"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content:        []byte("world"),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Content:        s.largeTestContent(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "VALUE_TOO_LARGE")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Content:        []byte("world"),
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestLookupIn() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte(`{"foo":"bar", "arr":[1,2,3], "num": 14}`))

		testBasicSpec := func(spec *kv_v1.LookupInRequest_Spec) *kv_v1.LookupInResponse {
			resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Specs:          []*kv_v1.LookupInRequest_Spec{spec},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			return resp
		}

		s.Run("Get", func() {
			resp := testBasicSpec(&kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "foo",
			})

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), []byte(`"bar"`), resp.Specs[0].Content)
		})

		s.Run("Count", func() {
			resp := testBasicSpec(&kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_COUNT,
				Path:      "arr",
			})

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), []byte(`3`), resp.Specs[0].Content)
		})

		s.Run("Exists", func() {
			resp := testBasicSpec(&kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_EXISTS,
				Path:      "foo",
			})

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), []byte(`true`), resp.Specs[0].Content)
		})

		s.Run("NotExists", func() {
			resp := testBasicSpec(&kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_EXISTS,
				Path:      "missingelement",
			})

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), []byte(`false`), resp.Specs[0].Content)
		})

		s.Run("GetVattr", func() {
			trueBool := true

			resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Specs: []*kv_v1.LookupInRequest_Spec{
					{
						Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
						Path:      "$document.exptime",
						Flags: &kv_v1.LookupInRequest_Spec_Flags{
							Xattr: &trueBool,
						},
					},
				},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.NotNil(s.T(), resp.Specs[0].Content)
		})

		s.Run("GetEmptyPath", func() {
			resp := testBasicSpec(&kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "",
			})

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), []byte(`{"foo":"bar", "arr":[1,2,3], "num": 14}`), resp.Specs[0].Content)
		})
	})

	s.Run("NoSpecs", func() {
		_, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Specs:          nil,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DocTooDeep", func() {
		docId := s.binaryDocId(s.tooDeepJson())

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "foo",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.FailedPrecondition)
		assertStatusProtoDetails(s.T(), resp.Specs[0].Status, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "DOC_TOO_DEEP")
		})
	})

	s.Run("DocNotJSON", func() {
		docId := s.binaryDocId([]byte(`hello`))

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "foo",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.FailedPrecondition)
		assertStatusProtoDetails(s.T(), resp.Specs[0].Status, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "DOC_NOT_JSON")
		})
	})

	s.Run("PathNotFound", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "b",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.NotFound)
		assertStatusProtoDetails(s.T(), resp.Specs[0].Status, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "path")
		})
	})

	s.Run("PathMismatch", func() {
		docId := s.binaryDocId([]byte(`{"a":[1, 2, 3]}`))

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "a.b",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.FailedPrecondition)
		assertStatusProtoDetails(s.T(), resp.Specs[0].Status, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "PATH_MISMATCH")
		})
	})

	s.Run("PathInvalid", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "]invalid",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.InvalidArgument)
	})

	s.Run("PathTooBig", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      s.tooDeepJsonPath(),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.InvalidArgument)
	})

	s.Run("UnknownVattr", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))
		trueBool := true

		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "$invalidvattr.lol",
					Flags: &kv_v1.LookupInRequest_Spec_Flags{
						Xattr: &trueBool,
					},
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.InvalidArgument)
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "foo",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "foo",
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "foo",
				},
			},
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestMutateIn() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	checkDocumentPath := func(docId, path string, content []byte) {
		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      path,
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)

		assert.Len(s.T(), resp.Specs, 1)
		if content != nil {
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), content, resp.Specs[0].Content)
		} else {
			assert.Equal(s.T(), int32(codes.NotFound), resp.Specs[0].Status.Code)
		}
	}

	s.Run("Basic", func() {
		testBasicSpec := func(docId string, spec *kv_v1.MutateInRequest_Spec) {
			resp, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Specs:          []*kv_v1.MutateInRequest_Spec{spec},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)
		}

		s.Run("Insert", func() {
			docId := s.binaryDocId([]byte(`{"foo": 14}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
				Path:      "newfoo",
				Content:   []byte(`"baz"`),
			})

			checkDocumentPath(docId, "newfoo", []byte(`"baz"`))
		})

		s.Run("Upsert", func() {
			docId := s.binaryDocId([]byte(`{"foo": 14}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
				Path:      "foo",
				Content:   []byte(`"baz"`),
			})

			checkDocumentPath(docId, "foo", []byte(`"baz"`))
		})

		s.Run("Replace", func() {
			docId := s.binaryDocId([]byte(`{"foo": 14}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_REPLACE,
				Path:      "foo",
				Content:   []byte(`"baz"`),
			})

			checkDocumentPath(docId, "foo", []byte(`"baz"`))
		})

		s.Run("ReplaceEmptyPath", func() {
			docId := s.binaryDocId([]byte(`{"foo": 14}`))
			content := []byte(`{"bar": 28}`)

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_REPLACE,
				Path:      "",
				Content:   content,
			})

			checkDocumentPath(docId, "", content)
		})

		s.Run("Remove", func() {
			docId := s.binaryDocId([]byte(`{"foo": 14}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_REMOVE,
				Path:      "foo",
			})

			checkDocumentPath(docId, "foo", nil)
		})

		s.Run("RemoveEmptyPath", func() {
			docId := s.binaryDocId([]byte(`{"foo": 14}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_REMOVE,
				Path:      "",
			})

			_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))

			assertRpcStatus(s.T(), err, codes.NotFound)
		})

		s.Run("ArrayAppend", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND,
				Path:      "arr",
				Content:   []byte(`4`),
			})

			checkDocumentPath(docId, "arr", []byte(`[1,2,3,4]`))
		})

		s.Run("ArrayAppendMulti", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND,
				Path:      "arr",
				Content:   []byte(`4,5`),
			})

			checkDocumentPath(docId, "arr", []byte(`[1,2,3,4,5]`))
		})

		s.Run("ArrayPrepend", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_PREPEND,
				Path:      "arr",
				Content:   []byte(`4`),
			})

			checkDocumentPath(docId, "arr", []byte(`[4,1,2,3]`))
		})

		s.Run("ArrayPrependMulti", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_PREPEND,
				Path:      "arr",
				Content:   []byte(`4,5`),
			})

			checkDocumentPath(docId, "arr", []byte(`[4,5,1,2,3]`))
		})

		s.Run("ArrayInsert", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_INSERT,
				Path:      "arr[1]",
				Content:   []byte(`4`),
			})

			checkDocumentPath(docId, "arr", []byte(`[1,4,2,3]`))
		})

		s.Run("ArrayInsertMulti", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_INSERT,
				Path:      "arr[1]",
				Content:   []byte(`4,5`),
			})

			checkDocumentPath(docId, "arr", []byte(`[1,4,5,2,3]`))
		})

		s.Run("ArrayAddUnique", func() {
			docId := s.binaryDocId([]byte(`{"arr": [1,2,3]}`))

			testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE,
				Path:      "arr",
				Content:   []byte(`4`),
			})

			checkDocumentPath(docId, "arr", []byte(`[1,2,3,4]`))
		})

		s.Run("Counter", func() {
			s.Run("Increment", func() {
				docId := s.binaryDocId([]byte(`{"num": 5}`))

				testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(`1`),
				})

				checkDocumentPath(docId, "num", []byte(`6`))
			})

			s.Run("IncrementMax", func() {
				docId := s.binaryDocId([]byte(`{"num": 0}`))
				maxValStr := fmt.Sprintf("%d", math.MaxInt64)

				testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(maxValStr),
				})

				checkDocumentPath(docId, "num", []byte(maxValStr))
			})

			s.Run("Decrement", func() {
				docId := s.binaryDocId([]byte(`{"num": 5}`))

				testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(`-1`),
				})

				checkDocumentPath(docId, "num", []byte(`4`))
			})

			s.Run("DecrementMax", func() {
				docId := s.binaryDocId([]byte(`{"num": 0}`))
				// We test with MinInt64+1 due to MB-57177 which caused MinInt64 to
				// fail until it was fixed and our tests run across multiple versions.
				minValStr := fmt.Sprintf("%d", math.MinInt64+1)

				testBasicSpec(docId, &kv_v1.MutateInRequest_Spec{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(minValStr),
				})

				checkDocumentPath(docId, "num", []byte(minValStr))
			})
		})
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Cas:            &docCas,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "a",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)
	})

	s.Run("NoSpecs", func() {
		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Specs:          nil,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ArrayAddUniqueDuplicate", func() {
		docId := s.binaryDocId([]byte(`{"arr":[1,2,3]}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE,
					Path:      "arr",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "path")
		})
	})

	s.Run("ValueNotJson", func() {
		docId := s.binaryDocId([]byte(`{"a":{}}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "a",
					Content:   []byte(`{a}`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "WOULD_INVALIDATE_JSON")
		})
	})

	s.Run("ValueTooDeep", func() {
		docId := s.binaryDocId([]byte(`{"a":{}}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "a",
					Content:   s.tooDeepJson(),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DeltaInvalid", func() {
		docId := s.binaryDocId([]byte(`{"num":5}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(`{}`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DeltaOverflowsPathValue", func() {
		// We subtract 1 from MaxInt64 to get a number thats valid, but can't be added
		// and still maintain a valid value
		tooBigValueStr := fmt.Sprintf("%d", math.MaxInt64-1)
		docId := s.binaryDocId([]byte(`{"num":` + tooBigValueStr + `}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(tooBigValueStr),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "WOULD_INVALIDATE_JSON")
		})
	})

	s.Run("DeltaOnOverflowingValue", func() {
		// we add "99" to the start of the maximum number to make it too big
		tooBigValueStr := "99" + fmt.Sprintf("%d", math.MaxInt64)
		docId := s.binaryDocId([]byte(`{"num":` + tooBigValueStr + `}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "num",
					Content:   []byte(`1`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "PATH_VALUE_OUT_OF_RANGE")
		})
	})

	s.Run("DocTooDeep", func() {
		docId := s.binaryDocId(s.tooDeepJson())

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "DOC_TOO_DEEP")
		})
	})

	s.Run("DocNotJSON", func() {
		docId := s.binaryDocId([]byte(`hello`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "DOC_NOT_JSON")
		})
	})

	s.Run("PathExistsWithInsert", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
					Path:      "a",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "path")
		})
	})

	s.Run("PathNotFoundWithReplace", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_REPLACE,
					Path:      "b",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "path")
		})
	})

	s.Run("PathMismatch", func() {
		docId := s.binaryDocId([]byte(`{"a":[1, 2, 3]}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "a.b",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "PATH_MISMATCH")
		})
	})

	s.Run("PathInvalid", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "]invalid",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("PathTooBig", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      s.tooDeepJsonPath(),
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("CasMismatch", func() {
		docId, docCas := s.testDocIdAndCas()
		incorrectCas := s.incorrectCas(docCas)

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Cas:            &incorrectCas,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "a",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), d.Reason, "CAS_MISMATCH")
		})
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   []byte(`"baz"`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "document")
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   []byte(`"baz"`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "LOCKED")
		})
	})

	s.Run("ExpirySecs", func() {
		semantic := kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT
		s.Run("ConversionUnder30Days", func() {
			docId := s.randomDocId()
			_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Specs: []*kv_v1.MutateInRequest_Spec{
					{
						Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
						Path:      "a",
						Content:   []byte(`2`),
					},
				},
				Expiry:        &kv_v1.MutateInRequest_ExpirySecs{ExpirySecs: 5},
				StoreSemantic: &semantic,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.OK)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte(`{"a":2}`),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: 5 + 1,
					MinSecs: 0,
				},
			})
		})

		s.Run("Conversion30Days", func() {
			docId := s.randomDocId()
			_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Specs: []*kv_v1.MutateInRequest_Spec{
					{
						Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
						Path:      "a",
						Content:   []byte(`2`),
					},
				},
				Expiry:        &kv_v1.MutateInRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
				StoreSemantic: &semantic,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.OK)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte(`{"a":2}`),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((29 * 24 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ConversionOver30Days", func() {
			docId := s.randomDocId()
			_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Specs: []*kv_v1.MutateInRequest_Spec{
					{
						Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
						Path:      "a",
						Content:   []byte(`2`),
					},
				},
				Expiry:        &kv_v1.MutateInRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
				StoreSemantic: &semantic,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.OK)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        []byte(`{"a":2}`),
				ContentFlags:   0,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
					MinSecs: int((30 * 24 * time.Hour).Seconds()),
				},
			})
		})
	})

	s.Run("ValueTooLargeNewDoc", func() {
		semantic := kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT

		// We have to JSON marshal this, and then truncate it so that it doesn't overflow
		// the grpc size limit.
		b, err := json.Marshal(s.largeTestContent())
		require.NoError(s.T(), err)
		b = append(b[:21000000], []byte(`"`)...)

		_, err = kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.randomDocId(),
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   b,
				},
			},
			StoreSemantic: &semantic,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ValueTooLargeExistingDoc", func() {
		// We have to JSON marshal this, and then truncate it so that it doesn't overflow
		// the grpc size limit.
		b, err := json.Marshal(s.largeTestContent())
		require.NoError(s.T(), err)
		b = append(b[:21000000], []byte(`"`)...)

		_, err = kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   b,
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), d.Violations[0].Type, "VALUE_TOO_LARGE")
		})
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		return kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:      opts.BucketName,
			ScopeName:       opts.ScopeName,
			CollectionName:  opts.CollectionName,
			Key:             s.randomDocId(),
			Cas:             nil,
			DurabilityLevel: nil,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      "foo",
					Content:   []byte(`"baz"`),
				},
			},
		}, opts.CallOptions...)
	})
}

func (s *GatewayOpsTestSuite) TestGetAllReplicas() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		resp, err := kvClient.GetAllReplicas(context.Background(), &kv_v1.GetAllReplicasRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		numResults := 0
		for {
			itemResp, err := resp.Recv()
			if err != nil {
				break
			}

			assertValidCas(s.T(), itemResp.Cas)
			assert.Equal(s.T(), itemResp.Content, TEST_CONTENT)
			assert.Equal(s.T(), itemResp.ContentFlags, TEST_CONTENT_FLAGS)
			numResults++
		}

		// since the document is at least written to the master, we must get
		// at least a single response, and more is acceptable.
		require.Greater(s.T(), numResults, 0)
	})

	s.Run("DocNotFound", func() {
		resp, err := kvClient.GetAllReplicas(context.Background(), &kv_v1.GetAllReplicasRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            uuid.NewString()[:6],
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		numResults := 0
		for {
			_, err := resp.Recv()
			if err != nil {
				break
			}

			numResults++
		}

		// This document does not exist on any node so resp.Recv should have immediately errored.
		require.Zero(s.T(), numResults)
	})

	s.RunCommonErrorCases(func(opts *commonErrorTestData) (interface{}, error) {
		resp, err := kvClient.GetAllReplicas(context.Background(), &kv_v1.GetAllReplicasRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            s.randomDocId(),
		}, opts.CallOptions...)
		requireRpcSuccess(s.T(), resp, err)

		var results []interface{}
		for {
			itemResp, err := resp.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return nil, err
			}

			results = append(results, itemResp)
		}

		return results, nil
	})
}

func TestGatewayOps(t *testing.T) {
	suite.Run(t, new(GatewayOpsTestSuite))
}
