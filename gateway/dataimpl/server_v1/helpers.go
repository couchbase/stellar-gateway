package server_v1

import (
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func casFromGocb(cas gocb.Cas) uint64 {
	return uint64(cas)
}

func casToGocb(cas *uint64) gocb.Cas {
	return gocb.Cas(*cas)
}

func timeFromGo(when time.Time) *timestamppb.Timestamp {
	// This is a workaround for a bug in Go where its Zero return values are not
	// actually matched with IsZero().
	// TODO(brett19): Remove this workaround when gocbcore is fixed.
	if when.Equal(time.Unix(0, 0)) {
		return nil
	}

	if when.IsZero() {
		return nil
	}
	return timestamppb.New(when)
}

func timeToGo(ts *timestamppb.Timestamp) time.Time {
	return ts.AsTime()
}

func durationFromGo(dura time.Duration) *durationpb.Duration {
	return durationpb.New(dura)
}

func durationToGo(d *durationpb.Duration) time.Duration {
	return d.AsDuration()
}

func tokenFromGocb(token *gocb.MutationToken) *kv_v1.MutationToken {
	if token == nil {
		return nil
	}

	return &kv_v1.MutationToken{
		BucketName:  token.BucketName(),
		VbucketId:   uint32(token.PartitionID()),
		VbucketUuid: token.PartitionUUID(),
		SeqNo:       token.SequenceNumber(),
	}
}

func durabilityLevelFromGocb(dl gocb.DurabilityLevel) (kv_v1.DurabilityLevel, *status.Status) {
	switch dl {
	case gocb.DurabilityLevelMajority:
		return kv_v1.DurabilityLevel_MAJORITY, nil
	case gocb.DurabilityLevelMajorityAndPersistOnMaster:
		return kv_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE, nil
	case gocb.DurabilityLevelPersistToMajority:
		return kv_v1.DurabilityLevel_PERSIST_TO_MAJORITY, nil
	}

	return kv_v1.DurabilityLevel(0), status.New(codes.InvalidArgument, "invalid durability level received")
}

func durabilityLevelToGocb(dl kv_v1.DurabilityLevel) (gocb.DurabilityLevel, *status.Status) {
	switch dl {
	case kv_v1.DurabilityLevel_MAJORITY:
		return gocb.DurabilityLevelMajority, nil
	case kv_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return gocb.DurabilityLevelMajorityAndPersistOnMaster, nil
	case kv_v1.DurabilityLevel_PERSIST_TO_MAJORITY:
		return gocb.DurabilityLevelPersistToMajority, nil
	}

	// TODO(brett19): We should attach the field reference information here indicating
	// what specific field the user incorrectly specified.
	return gocb.DurabilityLevel(0), status.New(codes.InvalidArgument, "invalid durability level specified")
}

func bucketTypeFromGocb(t gocb.BucketType) (admin_bucket_v1.BucketType, *status.Status) {
	switch t {
	case gocb.MemcachedBucketType:
		return admin_bucket_v1.BucketType_MEMCACHED, nil
	case gocb.CouchbaseBucketType:
		return admin_bucket_v1.BucketType_COUCHBASE, nil
	case gocb.EphemeralBucketType:
		return admin_bucket_v1.BucketType_EPHEMERAL, nil
	}

	return admin_bucket_v1.BucketType(0), status.New(codes.InvalidArgument, "invalid bucket type received")
}

func bucketTypeToGocb(t admin_bucket_v1.BucketType) (gocb.BucketType, *status.Status) {
	switch t {
	case admin_bucket_v1.BucketType_MEMCACHED:
		return gocb.MemcachedBucketType, nil
	case admin_bucket_v1.BucketType_COUCHBASE:
		return gocb.CouchbaseBucketType, nil
	case admin_bucket_v1.BucketType_EPHEMERAL:
		return gocb.EphemeralBucketType, nil
	}

	return gocb.BucketType(""), status.New(codes.InvalidArgument, "invalid bucket type specified")
}

func evictionModeFromGocb(em gocb.EvictionPolicyType) (admin_bucket_v1.EvictionMode, *status.Status) {
	switch em {
	case gocb.EvictionPolicyTypeFull:
		return admin_bucket_v1.EvictionMode_FULL, nil
	case gocb.EvictionPolicyTypeValueOnly:
		return admin_bucket_v1.EvictionMode_VALUE_ONLY, nil
	case gocb.EvictionPolicyTypeNotRecentlyUsed:
		return admin_bucket_v1.EvictionMode_NOT_RECENTLY_USED, nil
	case gocb.EvictionPolicyTypeNoEviction:
		return admin_bucket_v1.EvictionMode_NONE, nil
	}

	return admin_bucket_v1.EvictionMode(0), status.New(codes.InvalidArgument, "invalid eviction mode received")
}

func evictionModeToGocb(em admin_bucket_v1.EvictionMode) (gocb.EvictionPolicyType, *status.Status) {
	switch em {
	case admin_bucket_v1.EvictionMode_FULL:
		return gocb.EvictionPolicyTypeFull, nil
	case admin_bucket_v1.EvictionMode_VALUE_ONLY:
		return gocb.EvictionPolicyTypeValueOnly, nil
	case admin_bucket_v1.EvictionMode_NOT_RECENTLY_USED:
		return gocb.EvictionPolicyTypeNotRecentlyUsed, nil
	case admin_bucket_v1.EvictionMode_NONE:
		return gocb.EvictionPolicyTypeNoEviction, nil
	}

	return gocb.EvictionPolicyType(""), status.New(codes.InvalidArgument, "invalid eviction mode specified")
}

func compressionModeFromGocb(cm gocb.CompressionMode) (admin_bucket_v1.CompressionMode, *status.Status) {
	switch cm {
	case gocb.CompressionModeOff:
		return admin_bucket_v1.CompressionMode_OFF, nil
	case gocb.CompressionModePassive:
		return admin_bucket_v1.CompressionMode_PASSIVE, nil
	case gocb.CompressionModeActive:
		return admin_bucket_v1.CompressionMode_ACTIVE, nil

	}

	return admin_bucket_v1.CompressionMode(0), status.New(codes.InvalidArgument, "invalid compression mode received")
}

func compressionModeToGocb(cm admin_bucket_v1.CompressionMode) (gocb.CompressionMode, *status.Status) {
	switch cm {
	case admin_bucket_v1.CompressionMode_OFF:
		return gocb.CompressionModeOff, nil
	case admin_bucket_v1.CompressionMode_PASSIVE:
		return gocb.CompressionModePassive, nil
	case admin_bucket_v1.CompressionMode_ACTIVE:
		return gocb.CompressionModeActive, nil
	}

	return gocb.CompressionMode(""), status.New(codes.InvalidArgument, "invalid compression mode specified")
}

func storageBackendFromGocb(sb gocb.StorageBackend) (admin_bucket_v1.StorageBackend, *status.Status) {
	switch sb {
	case gocb.StorageBackendCouchstore:
		return admin_bucket_v1.StorageBackend_COUCHSTORE, nil
	case gocb.StorageBackendMagma:
		return admin_bucket_v1.StorageBackend_MAGMA, nil
	}

	return admin_bucket_v1.StorageBackend(0), status.New(codes.InvalidArgument, "invalid storage backend received")
}

func storageBackendToGocb(sb admin_bucket_v1.StorageBackend) (gocb.StorageBackend, *status.Status) {
	switch sb {
	case admin_bucket_v1.StorageBackend_COUCHSTORE:
		return gocb.StorageBackendCouchstore, nil
	case admin_bucket_v1.StorageBackend_MAGMA:
		return gocb.StorageBackendMagma, nil
	}

	return gocb.StorageBackend(""), status.New(codes.InvalidArgument, "invalid storage backend specified")
}

func conflictResolutionTypeFromGocb(t gocb.ConflictResolutionType) (admin_bucket_v1.ConflictResolutionType, *status.Status) {
	switch t {
	case gocb.ConflictResolutionTypeTimestamp:
		return admin_bucket_v1.ConflictResolutionType_TIMESTAMP, nil
	case gocb.ConflictResolutionTypeSequenceNumber:
		return admin_bucket_v1.ConflictResolutionType_SEQUENCE_NUMBER, nil
	case gocb.ConflictResolutionTypeCustom:
		return admin_bucket_v1.ConflictResolutionType_CUSTOM, nil
	}

	return admin_bucket_v1.ConflictResolutionType(0), status.New(codes.InvalidArgument, "invalid conflict resolution type received")
}

func conflictResolutionTypeToGocb(t admin_bucket_v1.ConflictResolutionType) (gocb.ConflictResolutionType, *status.Status) {
	switch t {
	case admin_bucket_v1.ConflictResolutionType_TIMESTAMP:
		return gocb.ConflictResolutionTypeTimestamp, nil
	case admin_bucket_v1.ConflictResolutionType_SEQUENCE_NUMBER:
		return gocb.ConflictResolutionTypeSequenceNumber, nil
	case admin_bucket_v1.ConflictResolutionType_CUSTOM:
		return gocb.ConflictResolutionTypeCustom, nil
	}

	return gocb.ConflictResolutionType(""), status.New(codes.InvalidArgument, "invalid conflict resolution type specified")
}
