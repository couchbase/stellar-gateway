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

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
)

type commonErrorTestData struct {
	ScopeName      string
	BucketName     string
	CollectionName string
	CallOptions    []grpc.CallOption
	Key            string
	APIVersion     string
}

func (s *GatewayOpsTestSuite) RunCommonErrorCases(
	fn func(ctx context.Context, opts *commonErrorTestData) (interface{}, error),
) {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}

	ctx := context.Background()

	s.Run("CollectionMissing", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: "invalid-collection",
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key: s.randomDocId(),
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "collection", d.ResourceType)
		})
	})

	s.Run("ScopeMissing", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      "invalid-scope",
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key: s.randomDocId(),
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "scope", d.ResourceType)
		})
	})

	s.Run("BucketMissing", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     "invalid-bucket",
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key: s.randomDocId(),
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "bucket", d.ResourceType)
		})
	})

	s.Run("BadCredentials", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.badRpcCreds),
			},
			Key: s.randomDocId(),
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "user", d.ResourceType)
		})
	})

	s.Run("Unauthenticated", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions:    []grpc.CallOption{},
			Key:            s.randomDocId(),
		})
		assertRpcStatus(s.T(), err, codes.Unauthenticated)
	})

	s.Run("DocKeyTooLong", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key: s.docIdOfLen(251),
		})
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DocKeyTooShort", func() {
		_, err := fn(ctx, &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key: s.docIdOfLen(0),
		})
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	apiVerCtx := func(ctx context.Context, apiVersion string) context.Context {
		if apiVersion != "" {
			return metadata.AppendToOutgoingContext(ctx, "X-API-Version", apiVersion)
		}
		return ctx
	}

	s.Run("BadAPIVersion", func() {
		_, err := fn(apiVerCtx(ctx, "invalid-date"), &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key:        s.randomDocId(),
			APIVersion: "invalid-date",
		})
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("FutureAPIVersion", func() {
		_, err := fn(apiVerCtx(ctx, "2100-01-01"), &commonErrorTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			CallOptions: []grpc.CallOption{
				grpc.PerRPCCredentials(s.basicRpcCreds),
			},
			Key: s.randomDocId(),
		})
		assertRpcStatus(s.T(), err, codes.Unimplemented)
	})
}

func (s *GatewayOpsTestSuite) IterDurabilityLevelTests(
	fn func(durabilityLevel *kv_v1.DurabilityLevel),
) {
	DurabilityLevels := []*kv_v1.DurabilityLevel{
		ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY),
		ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE),
		ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY),
	}

	for _, durabilityLevel := range DurabilityLevels {
		s.Run(kv_v1.DurabilityLevel_name[int32(*durabilityLevel)], func() {
			fn(durabilityLevel)
		})
	}
}

func (s *GatewayOpsTestSuite) IterExpiryTests(fn func(expiry interface{}, fn func(*checkDocumentOptions))) {
	s.Run("ExpiryTime", func() {
		s.Run("Future", func() {
			fn(
				timestamppb.Timestamp{
					Seconds: time.Now().Add(time.Minute).Unix(),
				},
				func(opts *checkDocumentOptions) {
					opts.expiry = expiryCheckType_Future
				},
			)
		})

		s.Run("Past", func() {
			fn(
				timestamppb.Timestamp{
					Seconds: time.Now().Add(-time.Minute).Unix(),
				},
				func(opts *checkDocumentOptions) {
					opts.expiry = expiryCheckType_Past
					opts.Content = nil
				},
			)
		})
	})

	s.Run("ExpirySecs", func() {
		s.Run("ConversionUnder30Days", func() {
			fn(
				uint32(5),
				func(opts *checkDocumentOptions) {
					opts.expiry = expiryCheckType_Within
					opts.expiryBounds = expiryCheckTypeWithinBounds{
						MaxSecs: 5 + 1,
						MinSecs: 0,
					}
				},
			)
		})

		s.Run("Conversion30Days", func() {
			fn(
				uint32((time.Hour * 24 * 30).Seconds()),
				func(opts *checkDocumentOptions) {
					opts.expiry = expiryCheckType_Within
					opts.expiryBounds = expiryCheckTypeWithinBounds{
						MaxSecs: int((30 * 24 * time.Hour).Seconds()) + 1,
						MinSecs: int((29 * 24 * time.Hour).Seconds()),
					}
				},
			)
		})

		s.Run("ConversionOver30Days", func() {
			fn(
				uint32((time.Hour * 24 * 31).Seconds()),
				func(opts *checkDocumentOptions) {
					opts.expiry = expiryCheckType_Within
					opts.expiryBounds = expiryCheckTypeWithinBounds{
						MaxSecs: int((31 * 24 * time.Hour).Seconds()) + 1,
						MinSecs: int((20 * 24 * time.Hour).Seconds()),
					}
				},
			)
		})
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
		assert.Equal(s.T(), TEST_CONTENT, resp.GetContentUncompressed())
		assert.Nil(s.T(), resp.GetContentCompressed())
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)
	})

	s.Run("Compressed", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Compression:    kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Nil(s.T(), resp.GetContentUncompressed())
		assert.Equal(s.T(), TEST_CONTENT, s.decompressContent(resp.GetContentCompressed()))
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)
	})

	s.Run("CompressionOptional", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Compression:    kv_v1.CompressionEnabled_COMPRESSION_ENABLED_OPTIONAL.Enum(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), TEST_CONTENT, resp.GetContentUncompressed())
		assert.Nil(s.T(), resp.GetContentCompressed())
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
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
		assert.JSONEq(s.T(), `{"obj":{"num":14},"arr":[3,6,9,12]}`, string(resp.GetContentUncompressed()))
		assert.Nil(s.T(), resp.GetContentCompressed())
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
	})

	s.Run("ProjectMissing", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Project:        []string{"obj.missing"},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.JSONEq(s.T(), `null`, string(resp.GetContentUncompressed()))
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
	})

	s.Run("ProjectNestedMissing", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Project:        []string{"obj.num.nest"},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
	})

	s.Run("DocLocked", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		if !s.IsOlderServerVersion("8.0.0") {
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assert.Equal(s.T(), TEST_CONTENT, resp.GetContentUncompressed())
			assert.Nil(s.T(), resp.GetContentCompressed())
			assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
			assert.Nil(s.T(), resp.Expiry)
		} else {
			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
				assert.Len(s.T(), d.Violations, 1)
				assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
			})
		}
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
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("DocKeyMaxLen", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.docIdOfLen(250),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.Run("DocKeyMinLen", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.docIdOfLen(1),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Get(ctx, &kv_v1.GetRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
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

	s.Run("Compressed", func() {
		docId := s.randomDocId()
		resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.InsertRequest_ContentCompressed{
				ContentCompressed: s.compressContent(TEST_CONTENT),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
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

	s.Run("IncorrectlyCompressedContent", func() {
		docId := s.randomDocId()
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.InsertRequest_ContentCompressed{
				ContentCompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DocExists", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: s.largeTestContent(),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ValueTooLargeCompressed", func() {
		_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.InsertRequest_ContentCompressed{
				ContentCompressed: s.compressContent(s.largeTestRandomContent()),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Insert(ctx, &kv_v1.InsertRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.randomDocId()
		resp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags:    TEST_CONTENT_FLAGS,
			DurabilityLevel: durabilityLevel,
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

	s.IterExpiryTests(func(expiry interface{}, modifyOpts func(*checkDocumentOptions)) {
		docId := s.randomDocId()

		req := &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.InsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}

		switch v := expiry.(type) {
		case timestamppb.Timestamp:
			req.Expiry = &kv_v1.InsertRequest_ExpiryTime{
				ExpiryTime: &v,
			}
		case uint32:
			req.Expiry = &kv_v1.InsertRequest_ExpirySecs{
				ExpirySecs: v,
			}
		}

		resp, err := kvClient.Insert(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))

		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		defaultOpts := checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}
		modifyOpts(&defaultOpts)
		s.checkDocument(s.T(), defaultOpts)
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
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
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

	s.Run("Compressed", func() {
		docId := s.randomDocId()
		resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentCompressed{
				ContentCompressed: s.compressContent(TEST_CONTENT),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
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

	s.Run("IncorrectlyCompressedContent", func() {
		docId := s.randomDocId()
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentCompressed{
				ContentCompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
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
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: 5},
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
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((30 * 24 * time.Hour).Seconds())},
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
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((31 * 24 * time.Hour).Seconds())},
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

		s.Run("Preserve", func() {
			docId := s.randomDocId()

			// Create a doc with an expiry
			{
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
					Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((24 * time.Hour).Seconds())},
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), resp, err)
			}

			// Upsert the same document again, this time the expiry should be kept
			{
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), resp, err)
			}

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((24 * time.Hour).Seconds()) + 1,
					MinSecs: int((23 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("ZeroExpiryWithNew", func() {
			docId := s.randomDocId()

			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: 0},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_None,
			})
		})

		s.Run("ZeroWithExisting", func() {
			docId := s.randomDocId()

			// Create a doc with an expiry
			{
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
					Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((24 * time.Hour).Seconds())},
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), resp, err)
			}

			// Upsert the same document again, this time the expiry should be cleared
			{
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
					Expiry:       &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: 0},
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), resp, err)
			}

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_None,
			})
		})

		s.Run("PreserveWithSecsForNew", func() {
			docId := s.randomDocId()
			preserveExpiryOnExisting := true

			// New Doc should use new expiry
			{
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags:             TEST_CONTENT_FLAGS,
					Expiry:                   &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((24 * time.Hour).Seconds())},
					PreserveExpiryOnExisting: &preserveExpiryOnExisting,
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
						MaxSecs: int((24 * time.Hour).Seconds()) + 1,
						MinSecs: int((23 * time.Hour).Seconds()),
					},
				})
			}

			// Existing Doc should keep existing expiry
			{
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags:             TEST_CONTENT_FLAGS,
					Expiry:                   &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32((48 * time.Hour).Seconds())},
					PreserveExpiryOnExisting: &preserveExpiryOnExisting,
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
						MaxSecs: int((24 * time.Hour).Seconds()) + 1,
						MinSecs: int((23 * time.Hour).Seconds()),
					},
				})
			}
		})

		s.Run("NilWithPreserve", func() {
			preserveExpiryOnExisting := true
			_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            s.randomDocId(),
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags:             TEST_CONTENT_FLAGS,
				Expiry:                   nil,
				PreserveExpiryOnExisting: &preserveExpiryOnExisting,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.InvalidArgument)
		})

		s.Run("ZeroWithPreserve", func() {
			preserveExpiryOnExisting := true
			_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            s.randomDocId(),
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags:             TEST_CONTENT_FLAGS,
				Expiry:                   &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: 0},
				PreserveExpiryOnExisting: &preserveExpiryOnExisting,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.InvalidArgument)
		})
	})

	s.Run("ExpiryTime", func() {
		s.Run("Future", func() {
			docId := s.randomDocId()
			timeStamp := &timestamppb.Timestamp{
				Seconds: time.Now().Add(time.Minute).Unix(),
			}
			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry:       &kv_v1.UpsertRequest_ExpiryTime{ExpiryTime: timeStamp},
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
				expiry:         expiryCheckType_Future,
			})
		})

		s.Run("Past", func() {
			docId := s.randomDocId()
			timeStamp := &timestamppb.Timestamp{
				Seconds: time.Now().Add(-time.Minute).Unix(),
			}
			resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry:       &kv_v1.UpsertRequest_ExpiryTime{ExpiryTime: timeStamp},
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
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Past,
			})
		})

		s.Run("WithExisting", func() {
			docId := s.randomDocId()

			{
				timeStamp := &timestamppb.Timestamp{
					Seconds: time.Now().Add(time.Minute).Unix(),
				}
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
					Expiry:       &kv_v1.UpsertRequest_ExpiryTime{ExpiryTime: timeStamp},
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
					expiry:         expiryCheckType_Future,
				})
			}

			{
				timeStamp := &timestamppb.Timestamp{
					Seconds: time.Now().Add(-time.Minute).Unix(),
				}
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
					Expiry:       &kv_v1.UpsertRequest_ExpiryTime{ExpiryTime: timeStamp},
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
					ContentFlags:   TEST_CONTENT_FLAGS,
					expiry:         expiryCheckType_Future,
				})
			}
		})

		s.Run("PreserveWithExisting", func() {
			docId := s.randomDocId()

			{
				timeStamp := &timestamppb.Timestamp{
					Seconds: time.Now().Add(time.Minute).Unix(),
				}
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags: TEST_CONTENT_FLAGS,
					Expiry:       &kv_v1.UpsertRequest_ExpiryTime{ExpiryTime: timeStamp},
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
					expiry:         expiryCheckType_Future,
				})
			}

			{
				timeStamp := &timestamppb.Timestamp{
					Seconds: time.Now().Add(-time.Minute).Unix(),
				}
				resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags:             TEST_CONTENT_FLAGS,
					Expiry:                   &kv_v1.UpsertRequest_ExpiryTime{ExpiryTime: timeStamp},
					PreserveExpiryOnExisting: ptr.To(true),
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
					expiry:         expiryCheckType_Future,
				})
			}
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: s.largeTestContent(),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ValueTooLargeCompressed", func() {
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.UpsertRequest_ContentCompressed{
				ContentCompressed: s.compressContent(s.largeTestRandomContent()),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Upsert(ctx, &kv_v1.UpsertRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.randomDocId()
		resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags:    TEST_CONTENT_FLAGS,
			DurabilityLevel: durabilityLevel,
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
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: newContent,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
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

	s.Run("Compressed", func() {
		docId := s.testDocId()

		resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.ReplaceRequest_ContentCompressed{
				ContentCompressed: s.compressContent(newContent),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
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

	s.Run("IncorrectlyCompressedContent", func() {
		docId := s.randomDocId()

		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.ReplaceRequest_ContentCompressed{
				ContentCompressed: newContent,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: newContent,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
			Cas:          &docCas,
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
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: newContent,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
			Cas:          &incorrectCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Aborted)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
			assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
		})
	})

	s.Run("ZeroCas", func() {
		docId := s.randomDocId()
		docCas := uint64(0)

		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: newContent,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
			Cas:          &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.Run("ValueTooLarge", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: s.largeTestContent(),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ValueTooLargeCompressed", func() {
		_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Content: &kv_v1.ReplaceRequest_ContentCompressed{
				ContentCompressed: s.compressContent(s.largeTestRandomContent()),
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Replace(ctx, &kv_v1.ReplaceRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.testDocId()
		resp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags:    TEST_CONTENT_FLAGS,
			DurabilityLevel: durabilityLevel,
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

	s.IterExpiryTests(func(expiry interface{}, modifyOpts func(*checkDocumentOptions)) {
		docId := s.testDocId()

		req := &kv_v1.ReplaceRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.ReplaceRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		}

		switch v := expiry.(type) {
		case timestamppb.Timestamp:
			req.Expiry = &kv_v1.ReplaceRequest_ExpiryTime{
				ExpiryTime: &v,
			}
		case uint32:
			req.Expiry = &kv_v1.ReplaceRequest_ExpirySecs{
				ExpirySecs: v,
			}
		}

		resp, err := kvClient.Replace(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))

		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		defaultOpts := checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		}
		modifyOpts(&defaultOpts)
		s.checkDocument(s.T(), defaultOpts)
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
			assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
		})
	})

	s.Run("ZeroCas", func() {
		docId := s.randomDocId()
		docCas := uint64(0)

		_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Remove(ctx, &kv_v1.RemoveRequest{
			BucketName:      opts.BucketName,
			ScopeName:       opts.ScopeName,
			CollectionName:  opts.CollectionName,
			Key:             opts.Key,
			Cas:             nil,
			DurabilityLevel: nil,
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.testDocId()
		resp, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
			BucketName:      s.bucketName,
			ScopeName:       s.scopeName,
			CollectionName:  s.collectionName,
			Key:             docId,
			DurabilityLevel: durabilityLevel,
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

	s.Run("ExpiryTime", func() {
		s.Run("Future", func() {
			docId := s.testDocId()
			timeStamp := &timestamppb.Timestamp{
				Seconds: time.Now().Add(time.Minute).Unix(),
			}
			resp, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Expiry:         &kv_v1.TouchRequest_ExpiryTime{ExpiryTime: timeStamp},
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
				expiry:         expiryCheckType_Future,
			})
		})

		s.Run("Past", func() {
			docId := s.testDocId()
			timeStamp := &timestamppb.Timestamp{
				Seconds: time.Now().Add(-time.Minute).Unix(),
			}
			resp, err := kvClient.Touch(context.Background(), &kv_v1.TouchRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Expiry:         &kv_v1.TouchRequest_ExpiryTime{ExpiryTime: timeStamp},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        nil,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Past,
			})
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Touch(ctx, &kv_v1.TouchRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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
		assert.Equal(s.T(), TEST_CONTENT, resp.GetContentUncompressed())
		assert.Nil(s.T(), resp.GetContentCompressed())
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)

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

	s.Run("Compressed", func() {
		docId := s.testDocId()
		resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Expiry:         &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: 20},
			Compression:    kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Nil(s.T(), resp.GetContentUncompressed())
		assert.Equal(s.T(), TEST_CONTENT, s.decompressContent(resp.GetContentCompressed()))
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
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

	s.Run("ExpiryTime", func() {
		s.Run("Future", func() {
			docId := s.testDocId()
			timeStamp := &timestamppb.Timestamp{
				Seconds: time.Now().Add(time.Minute).Unix(),
			}
			resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Expiry:         &kv_v1.GetAndTouchRequest_ExpiryTime{ExpiryTime: timeStamp},
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
				expiry:         expiryCheckType_Future,
			})
		})

		s.Run("Past", func() {
			docId := s.testDocId()
			timeStamp := &timestamppb.Timestamp{
				Seconds: time.Now().Add(-time.Minute).Unix(),
			}
			resp, err := kvClient.GetAndTouch(context.Background(), &kv_v1.GetAndTouchRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Expiry:         &kv_v1.GetAndTouchRequest_ExpiryTime{ExpiryTime: timeStamp},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        nil,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Past,
			})
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.GetAndTouch(ctx, &kv_v1.GetAndTouchRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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
			LockTimeSecs:   30,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), TEST_CONTENT, resp.GetContentUncompressed())
		assert.Nil(s.T(), resp.GetContentCompressed())
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)

		// Validate that the document is locked and we can't do updates
		_, err = kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.Run("Compressed", func() {
		docId := s.testDocId()
		resp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			LockTimeSecs:   30,
			Compression:    kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS.Enum(),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Nil(s.T(), resp.GetContentUncompressed())
		assert.Equal(s.T(), TEST_CONTENT, s.decompressContent(resp.GetContentCompressed()))
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)
	})

	s.Run("DocMissing", func() {
		_, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.missingDocId(),
			LockTimeSecs:   5,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("DocLocked", func() {
		_, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.lockedDocId(),
			LockTimeSecs:   5,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
		assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
			assert.Len(s.T(), d.Violations, 1)
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.GetAndLock(ctx, &kv_v1.GetAndLockRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			LockTimeSecs:   5,
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
			LockTimeSecs:   30,
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
			LockTimeSecs:   30,
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
			assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
		})
	})

	s.Run("ZeroCas", func() {
		galDocId := s.testDocId()
		galResp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            galDocId,
			LockTimeSecs:   30,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), galResp, err)

		_, err = kvClient.Unlock(context.Background(), &kv_v1.UnlockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            galDocId,
			Cas:            0,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("DocNotLocked", func() {
		unlockedDocId, cas := s.testDocIdAndCas()

		// This test is a little bit strange as there was a "breaking" change in
		// server 7.6.  Prior to 7.6, we expect that the KV operation yields a
		// TmpFail, which leads to internal retries until timeout.  After 7.6,
		// we instead expect to receive a PreconditionFailure with a violation
		// type of "NOT_LOCKED".

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := kvClient.Unlock(ctx, &kv_v1.UnlockRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            unlockedDocId,
			Cas:            cas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		if s.IsOlderServerVersion("7.6.0") {
			assertRpcStatus(s.T(), err, codes.DeadlineExceeded)
		} else {
			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
				assert.Len(s.T(), d.Violations, 1)
				assert.Equal(s.T(), "NOT_LOCKED", d.Violations[0].Type)
			})
		}
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Unlock(ctx, &kv_v1.UnlockRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Exists(ctx, &kv_v1.ExistsRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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

	s.Run("WithInitialNegative", func() {
		docId := s.randomDocId()
		initialValue := int64(-2)

		_, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        &initialValue,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ZeroDelta", func() {
		docId := s.binaryDocId([]byte("5"))

		resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          0,
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.Run("NonNumericDoc", func() {
		docId := s.binaryDocId([]byte(`{"foo":"bar"}`))

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
			assert.Equal(s.T(), "DOC_NOT_NUMERIC", d.Violations[0].Type)
		})
	})

	s.Run("IllogicalExpiry", func() {
		docId := s.randomDocId()
		_, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Expiry:         &kv_v1.IncrementRequest_ExpirySecs{ExpirySecs: 1},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Increment(ctx, &kv_v1.IncrementRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Delta:          1,
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.binaryDocId([]byte("5"))

		resp, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
			BucketName:      s.bucketName,
			ScopeName:       s.scopeName,
			CollectionName:  s.collectionName,
			Key:             docId,
			Delta:           1,
			DurabilityLevel: durabilityLevel,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(6), resp.Content)

		checkDocument(docId, []byte("6"))
	})

	s.IterExpiryTests(func(expiry interface{}, modifyOpts func(*checkDocumentOptions)) {
		docId := s.randomDocId()

		req := &kv_v1.IncrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        ptr.To(int64(5)),
		}

		switch v := expiry.(type) {
		case timestamppb.Timestamp:
			req.Expiry = &kv_v1.IncrementRequest_ExpiryTime{
				ExpiryTime: &v,
			}
		case uint32:
			req.Expiry = &kv_v1.IncrementRequest_ExpirySecs{
				ExpirySecs: v,
			}
		}

		resp, err := kvClient.Increment(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))

		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		defaultOpts := checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("5"),
		}
		modifyOpts(&defaultOpts)
		s.checkDocument(s.T(), defaultOpts)
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

	s.Run("WithInitialNegative", func() {
		docId := s.randomDocId()
		initialValue := int64(-5)

		_, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        &initialValue,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("ZeroDelta", func() {
		docId := s.binaryDocId([]byte("5"))

		resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          0,
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.Run("NonNumericDoc", func() {
		docId := s.binaryDocId([]byte(`{"foo":"bar"}`))

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
			assert.Equal(s.T(), "DOC_NOT_NUMERIC", d.Violations[0].Type)
		})
	})

	s.Run("IllogicalExpiry", func() {
		docId := s.randomDocId()
		_, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Expiry:         &kv_v1.DecrementRequest_ExpirySecs{ExpirySecs: 1},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Decrement(ctx, &kv_v1.DecrementRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Delta:          1,
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.binaryDocId([]byte("7"))

		resp, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
			BucketName:      s.bucketName,
			ScopeName:       s.scopeName,
			CollectionName:  s.collectionName,
			Key:             docId,
			Delta:           1,
			DurabilityLevel: durabilityLevel,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), int64(6), resp.Content)

		checkDocument(docId, []byte("6"))
	})

	s.IterExpiryTests(func(expiry interface{}, modifyOpts func(*checkDocumentOptions)) {
		docId := s.randomDocId()

		req := &kv_v1.DecrementRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Delta:          1,
			Initial:        ptr.To(int64(5)),
		}

		switch v := expiry.(type) {
		case timestamppb.Timestamp:
			req.Expiry = &kv_v1.DecrementRequest_ExpiryTime{
				ExpiryTime: &v,
			}
		case uint32:
			req.Expiry = &kv_v1.DecrementRequest_ExpirySecs{
				ExpirySecs: v,
			}
		}

		resp, err := kvClient.Decrement(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))

		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		defaultOpts := checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("5"),
		}
		modifyOpts(&defaultOpts)
		s.checkDocument(s.T(), defaultOpts)
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
			assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
		})
	})

	s.Run("ZeroCas", func() {
		docId := s.randomDocId()
		docCas := uint64(0)

		_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
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

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Append(ctx, &kv_v1.AppendRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content:        []byte("world"),
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.binaryDocId([]byte("hello"))

		resp, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
			BucketName:      s.bucketName,
			ScopeName:       s.scopeName,
			CollectionName:  s.collectionName,
			Key:             docId,
			Content:         []byte("world"),
			DurabilityLevel: durabilityLevel,
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
			assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
		})
	})

	s.Run("ZeroCas", func() {
		docId := s.randomDocId()
		docCas := uint64(0)

		_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content:        []byte("world"),
			Cas:            &docCas,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
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
			assert.Equal(s.T(), "VALUE_TOO_LARGE", d.Violations[0].Type)
		})
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.Prepend(ctx, &kv_v1.PrependRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
			Content:        []byte("world"),
		}, opts.CallOptions...)
	})

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.binaryDocId([]byte("world"))

		resp, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
			BucketName:      s.bucketName,
			ScopeName:       s.scopeName,
			CollectionName:  s.collectionName,
			Key:             docId,
			Content:         []byte("hello"),
			DurabilityLevel: durabilityLevel,
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

		s.Run("GetArrayElement", func() {
			resp := testBasicSpec(&kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "arr[1]",
			})

			assert.Len(s.T(), resp.Specs, 1)
			assert.Nil(s.T(), resp.Specs[0].Status)
			assert.Equal(s.T(), []byte(`2`), resp.Specs[0].Content)
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

	s.Run("TooManySpecs", func() {
		var specs []*kv_v1.LookupInRequest_Spec
		for i := 0; i < 17; i++ {
			specs = append(specs, &kv_v1.LookupInRequest_Spec{
				Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
				Path:      "a",
			})
		}

		_, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Specs:          specs,
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
			assert.Equal(s.T(), "DOC_TOO_DEEP", d.Violations[0].Type)
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
			assert.Equal(s.T(), "DOC_NOT_JSON", d.Violations[0].Type)
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
			assert.Equal(s.T(), "path", d.ResourceType)
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
			assert.Equal(s.T(), "PATH_MISMATCH", d.Violations[0].Type)
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
					Path:      s.jsonPathOfDepth(48),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)

		assert.Len(s.T(), resp.Specs, 1)
		assertStatusProto(s.T(), resp.Specs[0].Status, codes.InvalidArgument)
	})

	s.Run("PathTooLong", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		_, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      s.jsonPathOfLen(1025),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("DocLocked", func() {
		resp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
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
		if !s.IsOlderServerVersion("8.0.0") {
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
		} else {
			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
				assert.Len(s.T(), d.Violations, 1)
				assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
			})
		}
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.LookupIn(ctx, &kv_v1.LookupInRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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

	// BUG(ING-1211) - mutate in spec with empty path returns unhelpful error
	// s.Run("InsertWithEmptyPath", func() {
	// 	docId, docCas := s.testDocIdAndCas()
	// 	_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
	// 		BucketName:     s.bucketName,
	// 		ScopeName:      s.scopeName,
	// 		CollectionName: s.collectionName,
	// 		Key:            docId,
	// 		Cas:            &docCas,
	// 		Specs: []*kv_v1.MutateInRequest_Spec{
	// 			{
	// 				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
	// 				Path:      "",
	// 				Content:   []byte(`2`),
	// 			},
	// 		},
	// 	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	// 	assertRpcStatus(s.T(), err, codes.InvalidArgument)
	// })

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

	s.Run("ZeroCas", func() {
		docId := s.randomDocId()
		docCas := uint64(0)

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
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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

	s.Run("TooManySpecs", func() {
		var specs []*kv_v1.MutateInRequest_Spec
		for i := 0; i < 17; i++ {
			specs = append(specs, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
				Path:      "a",
				Content:   []byte(`2`),
			})
		}

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Specs:          specs,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("IncreaseNumberOfSpecs", func() {
		if s.IsOlderServerVersion("8.1.0") {
			s.T().Skip()
			return
		}

		ep, err := s.testClusterInfo.AdminClient.GetMgmtEndpoint(context.Background())
		require.NoError(s.T(), err)

		auth := &cbhttpx.BasicAuth{
			Username: ep.Username,
			Password: ep.Password,
		}

		defer func() {
			sixteen := 16
			err = cbmgmtx.Management{
				Transport: ep.RoundTripper,
				UserAgent: "",
				Endpoint:  ep.Endpoint,
				Auth:      auth,
			}.SetGlobalMemcachedSettings(context.Background(), &cbmgmtx.SetGlobalMemcachedSettingsOptions{
				SubdocMultiMaxPaths: &sixteen,
			})
			require.NoError(s.T(), err)
		}()

		maxPaths := 18

		err = cbmgmtx.Management{
			Transport: ep.RoundTripper,
			UserAgent: "",
			Endpoint:  ep.Endpoint,
			Auth:      auth,
		}.SetGlobalMemcachedSettings(context.Background(), &cbmgmtx.SetGlobalMemcachedSettingsOptions{
			SubdocMultiMaxPaths: &maxPaths,
		})
		require.NoError(s.T(), err)

		allMgmtEps, err := s.testClusterInfo.AdminClient.GetMgmtEndpoints()
		require.NoError(s.T(), err)

		s.Eventually(func() bool {
			for _, mgmtEp := range allMgmtEps {
				settings, err := cbmgmtx.Management{
					Transport: ep.RoundTripper,
					UserAgent: "",
					Endpoint:  "http://" + mgmtEp,
					Auth:      auth,
				}.GetGlobalMemcachedSettings(context.Background(), &cbmgmtx.GetGlobalMemcachedSettingsOptions{})
				require.NoError(s.T(), err)

				if setting, ok := settings[string(cbmgmtx.GlobalMemcachedSettingSubdocMultiMaxPaths)]; ok {
					paths := int(setting.(float64))
					if paths != maxPaths {
						return false
					}
				} else {
					return false
				}
			}

			return true
		}, 30*time.Second, 500*time.Millisecond)

		var specs []*kv_v1.MutateInRequest_Spec
		for i := 0; i < maxPaths; i++ {
			specs = append(specs, &kv_v1.MutateInRequest_Spec{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
				Path:      "a",
				Content:   []byte(`2`),
			})
		}

		_, err = kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			Specs:          specs,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)
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
			assert.Equal(s.T(), "path", d.ResourceType)
		})
	})

	s.Run("ArrayAddOnNonArrayPath", func() {
		docId := s.binaryDocId([]byte(`{"num":5}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE,
					Path:      "num",
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
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
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
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

	s.Run("CounterOpOnNonNumericField", func() {
		docId := s.binaryDocId([]byte(`{"string":"a-string"}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
					Path:      "string",
					Content:   []byte(`1`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.FailedPrecondition)
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
			assert.Equal(s.T(), "VALUE_OUT_OF_RANGE", d.Violations[0].Type)
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
			assert.Equal(s.T(), "PATH_VALUE_OUT_OF_RANGE", d.Violations[0].Type)
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
			assert.Equal(s.T(), "DOC_TOO_DEEP", d.Violations[0].Type)
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
			assert.Equal(s.T(), "DOC_NOT_JSON", d.Violations[0].Type)
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
			assert.Equal(s.T(), "path", d.ResourceType)
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
			assert.Equal(s.T(), "path", d.ResourceType)
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
			assert.Equal(s.T(), "PATH_MISMATCH", d.Violations[0].Type)
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
					Path:      s.jsonPathOfDepth(48),
					Content:   []byte(`2`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("PathTooLong", func() {
		docId := s.binaryDocId([]byte(`{"a":4}`))

		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
					Path:      s.jsonPathOfLen(1025),
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
			assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
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
			assert.Equal(s.T(), "document", d.ResourceType)
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
			assert.Equal(s.T(), "LOCKED", d.Violations[0].Type)
		})
	})

	s.IterExpiryTests(func(expiry interface{}, modifyOpts func(*checkDocumentOptions)) {
		docId := s.randomDocId()

		req := &kv_v1.MutateInRequest{
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
			StoreSemantic: ptr.To(kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT),
		}

		switch v := expiry.(type) {
		case timestamppb.Timestamp:
			req.Expiry = &kv_v1.MutateInRequest_ExpiryTime{
				ExpiryTime: &v,
			}
		case uint32:
			req.Expiry = &kv_v1.MutateInRequest_ExpirySecs{
				ExpirySecs: v,
			}
		}

		resp, err := kvClient.MutateIn(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))

		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)

		defaultOpts := checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte(`{"a":2}`),
		}
		modifyOpts(&defaultOpts)
		s.checkDocument(s.T(), defaultOpts)
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
			assert.Equal(s.T(), "VALUE_TOO_LARGE", d.Violations[0].Type)
		})
	})

	s.Run("DocExistsWithInsert", func() {
		ss := kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT
		_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
			StoreSemantic:  &ss,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
					Path:      "new_path",
					Content:   []byte(`"new_content"`),
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "document", d.ResourceType)
		})
	})

	s.Run("XattrInsert", func() {
		semantic := kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT
		docId := s.randomDocId()
		trueBool := true
		flags := kv_v1.MutateInRequest_Spec_Flags{Xattr: &trueBool}
		resp, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.MutateInRequest_Spec{
				{
					Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
					Path:      "testXattr",
					Content:   []byte(`2`),
					Flags:     &flags,
				},
			},
			StoreSemantic: &semantic,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("{}"),
		})

		lResp, err := kvClient.LookupIn(context.Background(), &kv_v1.LookupInRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Specs: []*kv_v1.LookupInRequest_Spec{
				{
					Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET,
					Path:      "testXattr",
					Flags: &kv_v1.LookupInRequest_Spec_Flags{
						Xattr: &trueBool,
					},
				},
			},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), lResp, err)
		assert.Len(s.T(), lResp.Specs, 1)
		assert.Nil(s.T(), lResp.Specs[0].Status)
		assert.Equal(s.T(), []byte(`2`), lResp.Specs[0].Content)
	})

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		return kvClient.MutateIn(ctx, &kv_v1.MutateInRequest{
			BucketName:      opts.BucketName,
			ScopeName:       opts.ScopeName,
			CollectionName:  opts.CollectionName,
			Key:             opts.Key,
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

	s.IterDurabilityLevelTests(func(durabilityLevel *kv_v1.DurabilityLevel) {
		docId := s.binaryDocId([]byte(`{"foo": 14}`))

		resp, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
			BucketName:      s.bucketName,
			ScopeName:       s.scopeName,
			CollectionName:  s.collectionName,
			Key:             docId,
			DurabilityLevel: durabilityLevel,
			Specs: []*kv_v1.MutateInRequest_Spec{{
				Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
				Path:      "newfoo",
				Content:   []byte(`"baz"`),
			}},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))

		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)
		checkDocumentPath(docId, "newfoo", []byte(`"baz"`))
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

		successful := 0
		numResponses := 0
		for {
			itemResp, err := resp.Recv()
			if err != nil {
				// EOF is not a response, it indicates that the stream has been
				// closed with no errors
				if !errors.Is(err, io.EOF) {
					numResponses++
				}
				break
			}

			assertValidCas(s.T(), itemResp.Cas)
			assert.Equal(s.T(), TEST_CONTENT, itemResp.Content)
			assert.Equal(s.T(), TEST_CONTENT_FLAGS, itemResp.ContentFlags)
			numResponses++
			successful++
		}

		// Since we are testing with two replicas we should get at least 2 responses,
		// in the case where both replica reads resturn an error.
		require.GreaterOrEqual(s.T(), numResponses, 2)

		// since the document is at least written to the master, we must get
		// at least a single response, and more is acceptable.
		require.Greater(s.T(), successful, 0)
	})

	s.Run("WaitForAllReplicas", func() {
		docId := s.testDocId()
		require.Eventually(s.T(), func() bool {
			resp, err := kvClient.GetAllReplicas(context.Background(), &kv_v1.GetAllReplicasRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)

			successful := 0
			for {
				itemResp, err := resp.Recv()
				if err != nil {
					break
				}

				assertValidCas(s.T(), itemResp.Cas)
				assert.Equal(s.T(), TEST_CONTENT, itemResp.Content)
				assert.Equal(s.T(), TEST_CONTENT_FLAGS, itemResp.ContentFlags)
				successful++
			}
			return successful == 3
		},
			time.Second*20,
			time.Second)
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

	s.RunCommonErrorCases(func(ctx context.Context, opts *commonErrorTestData) (interface{}, error) {
		resp, err := kvClient.GetAllReplicas(ctx, &kv_v1.GetAllReplicasRequest{
			BucketName:     opts.BucketName,
			ScopeName:      opts.ScopeName,
			CollectionName: opts.CollectionName,
			Key:            opts.Key,
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
