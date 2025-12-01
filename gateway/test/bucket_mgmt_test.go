package test

import (
	"context"
	"time"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type commonBucketMgmtErrorTestData struct {
	BucketName string
	Creds      credentials.PerRPCCredentials
}

func (s *GatewayOpsTestSuite) RunCommonBucketMgmtErrorCases(
	fn func(opts *commonBucketMgmtErrorTestData) (interface{}, error),
) {
	// TODO - ING-1191
	// s.Run("BlankBucketName", func() {
	// 	_, err := fn(&commonBucketMgmtErrorTestData{
	// 		BucketName: "",
	// 		Creds:      s.basicRpcCreds,
	// 	})
	// 	assertRpcStatus(s.T(), err, codes.InvalidArgument)
	// })
	// s.Run("BucketNameSpecialChars", func() {
	// 	_, err := fn(&commonBucketMgmtErrorTestData{
	// 		BucketName: "%?#",
	// 		Creds:      s.basicRpcCreds,
	// 	})
	// 	assertRpcStatus(s.T(), err, codes.InvalidArgument)
	// })
	s.Run("BadCredentials", func() {
		_, err := fn(&commonBucketMgmtErrorTestData{
			BucketName: uuid.NewString()[:6],
			Creds:      s.badRpcCreds,
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
	})
	s.Run("NoPermissions", func() {
		_, err := fn(&commonBucketMgmtErrorTestData{
			BucketName: uuid.NewString()[:6],
			Creds:      s.getNoPermissionRpcCreds(),
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
	})
}

func (s *GatewayOpsTestSuite) TestCreateBucket() {
	if !s.SupportsFeature(TestFeatureBucketManagement) {
		s.T().Skip()
	}

	adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)

	createdBuckets := &[]string{}
	s.T().Cleanup(func() {
		for _, bucketName := range *createdBuckets {
			_, _ = adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
				BucketName: bucketName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
		}
	})

	type createTest struct {
		description   string
		modifyDefault func(*admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest
		expect        codes.Code
	}

	createTests := []createTest{
		{
			description: "CouchbaseTypeDefaults",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				return def
			},
		},
		{
			description: "ReplicaIndexDisabled",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.ReplicaIndexes = ptr.To(false)
				return def
			},
		},
		{
			description: "FlushEnabled",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.FlushEnabled = ptr.To(true)
				return def
			},
		},
		{
			description: "WithNumReplicas",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.NumReplicas = ptr.To(uint32(2))
				return def
			},
		},
		{
			description: "EvictionModeFull",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL)
				return def
			},
		},
		{
			description: "EvictionModeValueOnly",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY)
				return def
			},
		},
		{
			description: "MaxExpiry",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.MaxExpirySecs = ptr.To(uint32(39))
				return def
			},
		},
		{
			description: "CompressionOff",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF)
				return def
			},
		},
		{
			description: "CompressionActive",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE)
				return def
			},
		},
		{
			description: "MajorityDurability",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY)
				return def
			},
		},
		{
			description: "PersistToActiveDurability",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE)
				return def
			},
		},
		{
			description: "PersistToMajorityDurability",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY)
				return def
			},
		},
		{
			description: "CouchstoreStorage",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE)
				return def
			},
		},
		{
			description: "MagmaStorage",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
				return def
			},
		},
		{
			description: "TimestampConflictResolution",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.ConflictResolutionType = ptr.To(admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP)
				return def
			},
		},
		{
			description: "HistoryRetentionDurationMagma",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
				def.HistoryRetentionDurationSecs = ptr.To(uint32(123))
				return def
			},
		},
		{
			description: "HistoryRetentionCollectionDefaultMagma",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.HistoryRetentionCollectionDefault = ptr.To(true)
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
				return def
			},
		},
		{
			description: "HistoryRetentionBytesMagma",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
				def.HistoryRetentionBytes = ptr.To(uint64(2147483648))
				return def
			},
		},
		{
			description: "CustomConflictResolution",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.ConflictResolutionType = ptr.To(admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_CUSTOM)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EvictionModeNru",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "AlreadyExists",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketName = "default"
				return def
			},
			expect: codes.AlreadyExists,
		},
		{
			description: "LessThan100MbRam",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.RamQuotaMb = ptr.To(uint64(99))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "MagmaLessThan1024MbRam",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.RamQuotaMb = ptr.To(uint64(1023))
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "TooManyReplicas",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.NumReplicas = ptr.To(uint32(4))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "InvalidBucketType",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = 3
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralTypeDefaults",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				return def
			},
		},
		{
			description: "EphemeralWithNruEviction",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED)
				return def
			},
		},
		{
			description: "EphemeralWithMaxExpirySet",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.MaxExpirySecs = ptr.To(uint32(39))
				return def
			},
		},
		{
			description: "EphemeralCompressionOff",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF)
				return def
			},
		},
		{
			description: "EphemeralCompressionActive",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE)
				return def
			},
		},
		{
			description: "EphemeralMajorityDurability",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY)
				return def
			},
		},
		{
			description: "EphemeralTimestampConflictResolution",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.ConflictResolutionType = ptr.To(admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP)
				return def
			},
		},
		{
			description: "EphemeralCustomConflictResolution",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.ConflictResolutionType = ptr.To(admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_CUSTOM)
				return def
			},
			expect: codes.InvalidArgument,
		},
		// TODO - ING-1189
		// {
		// 	description: "EphemeralCouchstoreStorage",
		// 	modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
		// 		def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
		// 		def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE)
		// 		return def
		// 	},
		// },
		// {
		// 	description: "EphemeralMagmaStorage",
		// 	modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
		// 		def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
		// 		def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
		// 		return def
		// 	},
		// },
		{
			description: "EphemeralPersistToActiveDurability",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralPersistToMajorityDurability",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralWithFullEviction",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralWithValueOnlyEviction",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionCollectionDefault",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.HistoryRetentionCollectionDefault = ptr.To(true)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionBytes",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.HistoryRetentionBytes = ptr.To(uint64(2147483648))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionDuration",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.HistoryRetentionDurationSecs = ptr.To(uint32(123))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionBytesMagmaLessThan2048Mb",
			modifyDefault: func(def *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.CreateBucketRequest {
				def.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
				def.HistoryRetentionBytes = ptr.To(uint64(2147483647))
				return def
			},
			expect: codes.InvalidArgument,
		},
	}

	for i := range createTests {
		t := createTests[i]
		s.Run(t.description, func() {
			defaultCreateRequest := admin_bucket_v1.CreateBucketRequest{
				BucketName: uuid.NewString()[:6],
				BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
			}
			req := t.modifyDefault(&defaultCreateRequest)
			resp, err := adminClient.CreateBucket(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			if t.expect != codes.OK {
				assertRpcStatus(s.T(), err, t.expect)
				return
			}
			requireRpcSuccess(s.T(), resp, err)

			defer func() {
				deleteResp, err := adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
					BucketName: req.BucketName,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), deleteResp, err)
			}()

			listResp, err := adminClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{},
				grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), listResp, err)

			var found *admin_bucket_v1.ListBucketsResponse_Bucket
			for _, b := range listResp.Buckets {
				if b.BucketName == req.BucketName {
					found = b
					break
				}
			}

			expected := defaultCouchbaseBucket()
			if req.StorageBackend != nil && *req.StorageBackend == admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA {
				expected = defaultMagmaBucket(expected)
			}

			if req.BucketType == admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL {
				expected = defaultEphemeralBucket(expected)
			}

			expected = expectedFromCreateRequest(expected, req)
			if assert.NotNil(s.T(), found, "Did not find bucket on cluster") {
				assert.Equal(s.T(), req.BucketType, found.BucketType)
				assert.Equal(s.T(), expected.RamQuotaMb, found.RamQuotaMb)
				assert.Equal(s.T(), expected.ReplicaIndexes, found.ReplicaIndexes)
				assert.Equal(s.T(), expected.FlushEnabled, found.FlushEnabled)
				assert.Equal(s.T(), expected.NumReplicas, found.NumReplicas)
				assert.Equal(s.T(), expected.EvictionMode, found.EvictionMode)
				assert.Equal(s.T(), expected.MaxExpirySecs, found.MaxExpirySecs)
				assert.Equal(s.T(), expected.CompressionMode, found.CompressionMode)
				assert.Equal(s.T(), expected.MinimumDurabilityLevel, found.MinimumDurabilityLevel)
				assert.Equal(s.T(), expected.StorageBackend, found.StorageBackend)
				assert.Equal(s.T(), expected.ConflictResolutionType, found.ConflictResolutionType)
				assert.Equal(s.T(), expected.HistoryRetentionCollectionDefault, found.HistoryRetentionCollectionDefault)
				assert.Equal(s.T(), expected.HistoryRetentionBytes, found.HistoryRetentionBytes)
				assert.Equal(s.T(), expected.HistoryRetentionDurationSecs, found.HistoryRetentionDurationSecs)
			}
		})
	}

	s.RunCommonBucketMgmtErrorCases(
		func(opts *commonBucketMgmtErrorTestData) (interface{}, error) {
			return adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
				BucketName: opts.BucketName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestDeleteBucket() {
	if !s.SupportsFeature(TestFeatureBucketManagement) {
		s.T().Skip()
	}

	adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)

	bucketName := uuid.NewString()[:6]
	resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName: bucketName,
		BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	type deleteTest struct {
		description   string
		modifyDefault func(*admin_bucket_v1.DeleteBucketRequest) *admin_bucket_v1.DeleteBucketRequest
		expect        codes.Code
	}

	deleteTests := []deleteTest{
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_bucket_v1.DeleteBucketRequest) *admin_bucket_v1.DeleteBucketRequest {
				def.BucketName = "missing-bucket"
				return def
			},
			expect: codes.NotFound,
		},
		{
			description: "Success",
			modifyDefault: func(def *admin_bucket_v1.DeleteBucketRequest) *admin_bucket_v1.DeleteBucketRequest {
				return def
			},
		},
	}

	for i := range deleteTests {
		t := deleteTests[i]
		s.Run(t.description, func() {
			defaultDeleteRequest := admin_bucket_v1.DeleteBucketRequest{
				BucketName: bucketName,
			}
			req := t.modifyDefault(&defaultDeleteRequest)
			resp, err := adminClient.DeleteBucket(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			if t.expect != codes.OK {
				assertRpcStatus(s.T(), err, t.expect)
				return
			}
			requireRpcSuccess(s.T(), resp, err)
		})
	}

	s.RunCommonBucketMgmtErrorCases(
		func(opts *commonBucketMgmtErrorTestData) (interface{}, error) {
			return adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
				BucketName: opts.BucketName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestFlushBucket() {
	if !s.SupportsFeature(TestFeatureBucketManagement) {
		s.T().Skip()
	}

	adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)

	bucketName := uuid.NewString()[:6]
	resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName:   bucketName,
		BucketType:   admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
		FlushEnabled: ptr.To(true),
		RamQuotaMb:   ptr.To[uint64](100),
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	s.T().Cleanup(func() {
		_, _ = adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	s.Run("Basic", func() {
		require.Eventually(s.T(), func() bool {
			resp, err := adminClient.FlushBucket(context.Background(), &admin_bucket_v1.FlushBucketRequest{
				BucketName: bucketName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			if err != nil {
				return false
			}

			requireRpcSuccess(s.T(), resp, err)
			return true
		}, 10*time.Second, 500*time.Millisecond)
	})

	type flushTest struct {
		description   string
		modifyDefault func(*admin_bucket_v1.FlushBucketRequest) *admin_bucket_v1.FlushBucketRequest
		expect        codes.Code
	}

	flushTests := []flushTest{
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_bucket_v1.FlushBucketRequest) *admin_bucket_v1.FlushBucketRequest {
				def.BucketName = "missing-bucket"
				return def
			},
			expect: codes.NotFound,
		},
	}

	for i := range flushTests {
		t := flushTests[i]
		s.Run(t.description, func() {
			defaultFlushRequest := admin_bucket_v1.FlushBucketRequest{
				BucketName: bucketName,
			}
			req := t.modifyDefault(&defaultFlushRequest)

			resp, err := adminClient.FlushBucket(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			if t.expect != codes.OK {
				assertRpcStatus(s.T(), err, t.expect)
				return
			}
			requireRpcSuccess(s.T(), resp, err)
		})
	}

	s.RunCommonBucketMgmtErrorCases(
		func(opts *commonBucketMgmtErrorTestData) (interface{}, error) {
			return adminClient.FlushBucket(context.Background(), &admin_bucket_v1.FlushBucketRequest{
				BucketName: opts.BucketName,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func (s *GatewayOpsTestSuite) TestFlushBucket_FlushDisabled() {
	if !s.SupportsFeature(TestFeatureBucketManagement) {
		s.T().Skip()
	}

	adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)

	bucketName := uuid.NewString()[:6]
	createResp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName:   bucketName,
		BucketType:   admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
		FlushEnabled: ptr.To(false),
		RamQuotaMb:   ptr.To[uint64](100),
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), createResp, err)

	s.T().Cleanup(func() {
		_, _ = adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
			BucketName: bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
	})

	var flushErr error
	require.Eventually(s.T(), func() bool {
		_, err = adminClient.FlushBucket(context.Background(), &admin_bucket_v1.FlushBucketRequest{
			BucketName: bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		if err == nil {
			flushErr = nil
			return true
		}

		errSt, _ := status.FromError(err)
		if errSt.Code() == codes.Unknown {
			return false
		}

		flushErr = err
		return true
	}, 10*time.Second, 500*time.Millisecond)

	assertRpcStatus(s.T(), flushErr, codes.FailedPrecondition)
	assertRpcErrorDetails(s.T(), flushErr, func(d *epb.PreconditionFailure) {
		assert.Len(s.T(), d.Violations, 1)
		assert.Equal(s.T(), "FLUSH_DISABLED", d.Violations[0].Type)
	})
}

func (s *GatewayOpsTestSuite) TestUpdateBucket() {
	if !s.SupportsFeature(TestFeatureBucketManagement) {
		s.T().Skip()
	}

	adminClient := admin_bucket_v1.NewBucketAdminServiceClient(s.gatewayConn)

	couchbaseBucket := uuid.NewString()[:6]
	resp, err := adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName: couchbaseBucket,
		BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	magmaBucket := uuid.NewString()[:6]
	resp, err = adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName:     magmaBucket,
		BucketType:     admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
		StorageBackend: ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA),
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	ephemeralBucket := uuid.NewString()[:6]
	resp, err = adminClient.CreateBucket(context.Background(), &admin_bucket_v1.CreateBucketRequest{
		BucketName: ephemeralBucket,
		BucketType: admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	createdBuckets := &[]string{couchbaseBucket, magmaBucket, ephemeralBucket}
	s.T().Cleanup(func() {
		for _, bucketName := range *createdBuckets {
			_, _ = adminClient.DeleteBucket(context.Background(), &admin_bucket_v1.DeleteBucketRequest{
				BucketName: bucketName,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
		}
	})

	type updateTest struct {
		description   string
		modifyDefault func(*admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest
		expect        codes.Code
	}

	updateTests := []updateTest{
		{
			description: "RamQuota",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.RamQuotaMb = ptr.To(uint64(123))
				return def
			},
		},
		{
			description: "NumReplicas",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.NumReplicas = ptr.To(uint32(2))
				return def
			},
		},
		{
			description: "TooManyReplicas",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.NumReplicas = ptr.To(uint32(5))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EnableFlush",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.FlushEnabled = ptr.To(true)
				return def
			},
		},
		{
			description: "EvictionModeFull",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL)
				return def
			},
		},
		{
			description: "EvictionModeValueOnly",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY)
				return def
			},
		},
		{
			description: "MaxExpirySecs",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.MaxExpirySecs = ptr.To(uint32(123))
				return def
			},
		},
		{
			description: "CompressionOff",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF)
				return def
			},
		},
		{
			description: "CompressionActive",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE)
				return def
			},
		},
		{
			description: "DurabilityLevelMajority",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY)
				return def
			},
		},
		{
			description: "DurabilityLevelMajorityAndPersistToActive",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE)
				return def
			},
		},
		{
			description: "DurabilityLevelPersistToMajority",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY)
				return def
			},
		},
		{
			description: "RamQuotaLessThan100Mb",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.RamQuotaMb = ptr.To(uint64(99))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "RamQuotaLessThanMinimumMagma",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket

				// The minimum RAM quota for magma buckets was lowered to 100 in
				// 8.0.0
				if s.IsOlderServerVersion("8.0.0") {
					def.RamQuotaMb = ptr.To(uint64(1023))
				} else {
					def.RamQuotaMb = ptr.To(uint64(99))
				}

				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "BucketNotFound",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = "missing-bucket"
				return def
			},
			expect: codes.NotFound,
		},
		{
			description: "HistoryRetentionCollectionDefault",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.HistoryRetentionCollectionDefault = ptr.To(true)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionBytes",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.HistoryRetentionBytes = ptr.To(uint64(1234))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionDurationSecs",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.HistoryRetentionDurationSecs = ptr.To(uint32(1234))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "HistoryRetentionCollectionDefaultMagma",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket
				def.HistoryRetentionCollectionDefault = ptr.To(false)
				return def
			},
		},
		{
			description: "HistoryRetentionBytesMagma",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket
				def.HistoryRetentionBytes = ptr.To(uint64(3000000000))
				return def
			},
		},
		{
			description: "HistoryRetentionDurationSecsMagma",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket
				def.HistoryRetentionDurationSecs = ptr.To(uint32(1234))
				return def
			},
		},
		{
			description: "CompressionOffMagma",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF)
				return def
			},
		},
		{
			description: "CompressionActiveMagma",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket
				def.CompressionMode = ptr.To(admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE)
				return def
			},
		},
		{
			description: "HistoryRetentionBytesMagmaLessThan2048Mb",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = magmaBucket
				def.HistoryRetentionBytes = ptr.To(uint64(2147483647))
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EvictionModeNru",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EvictionModeNone",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_NONE)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralEvictionMode",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = ephemeralBucket
				def.EvictionMode = ptr.To(admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralDurabilityLevelMajority",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = ephemeralBucket
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY)
				return def
			},
		},
		{
			description: "EphemeralDurabilityLevelMajorityAndPersistToActive",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = ephemeralBucket
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE)
				return def
			},
			expect: codes.InvalidArgument,
		},
		{
			description: "EphemeralDurabilityLevelPersistToMajority",
			modifyDefault: func(def *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.UpdateBucketRequest {
				def.BucketName = ephemeralBucket
				def.MinimumDurabilityLevel = ptr.To(kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY)
				return def
			},
			expect: codes.InvalidArgument,
		},
	}

	for i := range updateTests {
		t := updateTests[i]
		s.Run(t.description, func() {
			defaultUpdateRequest := admin_bucket_v1.UpdateBucketRequest{
				BucketName: couchbaseBucket,
			}
			req := t.modifyDefault(&defaultUpdateRequest)

			listResp, err := adminClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{},
				grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), listResp, err)

			var before *admin_bucket_v1.ListBucketsResponse_Bucket
			for _, b := range listResp.Buckets {
				if b.BucketName == req.BucketName {
					before = b
					break
				}
			}

			resp, err := adminClient.UpdateBucket(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
			if t.expect != codes.OK {
				assertRpcStatus(s.T(), err, t.expect)
				return
			}
			requireRpcSuccess(s.T(), resp, err)

			listResp, err = adminClient.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{},
				grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), listResp, err)

			var after *admin_bucket_v1.ListBucketsResponse_Bucket
			for _, b := range listResp.Buckets {
				if b.BucketName == req.BucketName {
					after = b
					break
				}
			}

			expected := expectedFromUpdateRequest(before, req)
			if assert.NotNil(s.T(), after, "Did not find bucket on cluster") {
				assert.Equal(s.T(), expected.RamQuotaMb, after.RamQuotaMb)
				assert.Equal(s.T(), expected.ReplicaIndexes, after.ReplicaIndexes)
				assert.Equal(s.T(), expected.FlushEnabled, after.FlushEnabled)
				assert.Equal(s.T(), expected.NumReplicas, after.NumReplicas)
				assert.Equal(s.T(), expected.EvictionMode, after.EvictionMode)
				assert.Equal(s.T(), expected.MaxExpirySecs, after.MaxExpirySecs)
				assert.Equal(s.T(), expected.CompressionMode, after.CompressionMode)
				assert.Equal(s.T(), expected.MinimumDurabilityLevel, after.MinimumDurabilityLevel)
				assert.Equal(s.T(), expected.StorageBackend, after.StorageBackend)
				assert.Equal(s.T(), expected.ConflictResolutionType, after.ConflictResolutionType)
				assert.Equal(s.T(), expected.HistoryRetentionCollectionDefault, after.HistoryRetentionCollectionDefault)
				assert.Equal(s.T(), expected.HistoryRetentionBytes, after.HistoryRetentionBytes)
				assert.Equal(s.T(), expected.HistoryRetentionDurationSecs, after.HistoryRetentionDurationSecs)
			}
		})
	}

	s.RunCommonBucketMgmtErrorCases(
		func(opts *commonBucketMgmtErrorTestData) (interface{}, error) {
			return adminClient.UpdateBucket(context.Background(), &admin_bucket_v1.UpdateBucketRequest{
				BucketName: couchbaseBucket,
			}, grpc.PerRPCCredentials(opts.Creds))
		})
}

func defaultCouchbaseBucket() *admin_bucket_v1.ListBucketsResponse_Bucket {
	return &admin_bucket_v1.ListBucketsResponse_Bucket{
		BucketType:                   admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE,
		RamQuotaMb:                   uint64(100),
		ReplicaIndexes:               true,
		FlushEnabled:                 false,
		NumReplicas:                  uint32(1),
		EvictionMode:                 admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY,
		MaxExpirySecs:                uint32(0),
		CompressionMode:              admin_bucket_v1.CompressionMode_COMPRESSION_MODE_PASSIVE,
		MinimumDurabilityLevel:       nil,
		StorageBackend:               ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE),
		ConflictResolutionType:       admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER,
		HistoryRetentionBytes:        ptr.To(uint64(0)),
		HistoryRetentionDurationSecs: ptr.To(uint32(0)),
	}
}

func defaultMagmaBucket(bucket *admin_bucket_v1.ListBucketsResponse_Bucket) *admin_bucket_v1.ListBucketsResponse_Bucket {
	bucket.StorageBackend = ptr.To(admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA)
	bucket.RamQuotaMb = uint64(1024)
	bucket.ReplicaIndexes = false
	bucket.EvictionMode = admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL
	bucket.HistoryRetentionCollectionDefault = ptr.To(true)
	return bucket
}

func defaultEphemeralBucket(bucket *admin_bucket_v1.ListBucketsResponse_Bucket) *admin_bucket_v1.ListBucketsResponse_Bucket {
	bucket.BucketType = admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL
	bucket.ReplicaIndexes = false
	bucket.EvictionMode = admin_bucket_v1.EvictionMode_EVICTION_MODE_NONE
	bucket.HistoryRetentionCollectionDefault = nil
	bucket.StorageBackend = nil
	return bucket
}

func expectedFromCreateRequest(expected *admin_bucket_v1.ListBucketsResponse_Bucket, req *admin_bucket_v1.CreateBucketRequest) *admin_bucket_v1.ListBucketsResponse_Bucket {
	if req.RamQuotaMb != nil {
		expected.RamQuotaMb = *req.RamQuotaMb
	}

	if req.ReplicaIndexes != nil {
		expected.ReplicaIndexes = *req.ReplicaIndexes
	}

	if req.EvictionMode != nil {
		expected.EvictionMode = *req.EvictionMode
	}

	if req.StorageBackend != nil {
		expected.StorageBackend = req.StorageBackend
	}

	if req.ConflictResolutionType != nil {
		expected.ConflictResolutionType = *req.ConflictResolutionType
	}

	if req.CompressionMode != nil {
		expected.CompressionMode = *req.CompressionMode
	}

	if req.MinimumDurabilityLevel != nil {
		expected.MinimumDurabilityLevel = req.MinimumDurabilityLevel
	}

	if req.FlushEnabled != nil {
		expected.FlushEnabled = *req.FlushEnabled
	}

	if req.NumReplicas != nil {
		expected.NumReplicas = *req.NumReplicas
	}

	if req.MaxExpirySecs != nil {
		expected.MaxExpirySecs = *req.MaxExpirySecs
	}

	if req.HistoryRetentionBytes != nil {
		expected.HistoryRetentionBytes = req.HistoryRetentionBytes
	}

	if req.HistoryRetentionDurationSecs != nil {
		expected.HistoryRetentionDurationSecs = req.HistoryRetentionDurationSecs
	}

	if req.HistoryRetentionCollectionDefault != nil {
		expected.HistoryRetentionCollectionDefault = req.HistoryRetentionCollectionDefault
	}

	return expected
}

func expectedFromUpdateRequest(expected *admin_bucket_v1.ListBucketsResponse_Bucket, req *admin_bucket_v1.UpdateBucketRequest) *admin_bucket_v1.ListBucketsResponse_Bucket {
	if req.RamQuotaMb != nil {
		expected.RamQuotaMb = *req.RamQuotaMb
	}

	if req.NumReplicas != nil {
		expected.NumReplicas = *req.NumReplicas
	}

	if req.FlushEnabled != nil {
		expected.FlushEnabled = *req.FlushEnabled
	}

	if req.EvictionMode != nil {
		expected.EvictionMode = *req.EvictionMode
	}

	if req.MaxExpirySecs != nil {
		expected.MaxExpirySecs = *req.MaxExpirySecs
	}

	if req.CompressionMode != nil {
		expected.CompressionMode = *req.CompressionMode
	}

	if req.MinimumDurabilityLevel != nil {
		expected.MinimumDurabilityLevel = req.MinimumDurabilityLevel
	}

	if req.HistoryRetentionBytes != nil {
		expected.HistoryRetentionBytes = req.HistoryRetentionBytes
	}

	if req.HistoryRetentionDurationSecs != nil {
		expected.HistoryRetentionDurationSecs = req.HistoryRetentionDurationSecs
	}

	if req.HistoryRetentionCollectionDefault != nil {
		expected.HistoryRetentionCollectionDefault = req.HistoryRetentionCollectionDefault
	}

	return expected
}
