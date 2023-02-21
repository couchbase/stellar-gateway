package server_v1

import (
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func timeExpiryToGocbcorex(expiry time.Time) uint32 {
	return uint32(expiry.Unix())
}

func tokenFromGocbcorex(bucketName string, token gocbcorex.MutationToken) *kv_v1.MutationToken {
	return &kv_v1.MutationToken{
		BucketName:  bucketName,
		VbucketId:   uint32(token.VbID),
		VbucketUuid: token.VbUuid,
		SeqNo:       token.SeqNo,
	}
}

func durabilityLevelFromMemdx(dl memdx.DurabilityLevel) (kv_v1.DurabilityLevel, *status.Status) {
	switch dl {
	case memdx.DurabilityLevelMajority:
		return kv_v1.DurabilityLevel_MAJORITY, nil
	case memdx.DurabilityLevelMajorityAndPersistToActive:
		return kv_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE, nil
	case memdx.DurabilityLevelPersistToMajority:
		return kv_v1.DurabilityLevel_PERSIST_TO_MAJORITY, nil
	}

	return kv_v1.DurabilityLevel(0), status.New(codes.InvalidArgument, "invalid durability level received")
}

func durabilityLevelToMemdx(dl kv_v1.DurabilityLevel) (memdx.DurabilityLevel, *status.Status) {
	switch dl {
	case kv_v1.DurabilityLevel_MAJORITY:
		return memdx.DurabilityLevelMajority, nil
	case kv_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return memdx.DurabilityLevelMajorityAndPersistToActive, nil
	case kv_v1.DurabilityLevel_PERSIST_TO_MAJORITY:
		return memdx.DurabilityLevelPersistToMajority, nil
	}

	// TODO(brett19): We should attach the field reference information here indicating
	// what specific field the user incorrectly specified.
	return memdx.DurabilityLevel(0), status.New(codes.InvalidArgument, "invalid durability level specified")
}

func durabilityLevelFromCbmgmtx(dl cbmgmtx.DurabilityLevel) (kv_v1.DurabilityLevel, *status.Status) {
	switch dl {
	case cbmgmtx.DurabilityLevelMajority:
		return kv_v1.DurabilityLevel_MAJORITY, nil
	case cbmgmtx.DurabilityLevelMajorityAndPersistOnMaster:
		return kv_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE, nil
	case cbmgmtx.DurabilityLevelPersistToMajority:
		return kv_v1.DurabilityLevel_PERSIST_TO_MAJORITY, nil
	}

	return kv_v1.DurabilityLevel(0), status.New(codes.InvalidArgument, "invalid durability level received")
}

func durabilityLevelToCbmgmtx(dl kv_v1.DurabilityLevel) (cbmgmtx.DurabilityLevel, *status.Status) {
	switch dl {
	case kv_v1.DurabilityLevel_MAJORITY:
		return cbmgmtx.DurabilityLevelMajority, nil
	case kv_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return cbmgmtx.DurabilityLevelMajorityAndPersistOnMaster, nil
	case kv_v1.DurabilityLevel_PERSIST_TO_MAJORITY:
		return cbmgmtx.DurabilityLevelPersistToMajority, nil
	}

	// TODO(brett19): We should attach the field reference information here indicating
	// what specific field the user incorrectly specified.
	return cbmgmtx.DurabilityLevel(""), status.New(codes.InvalidArgument, "invalid durability level specified")
}

func bucketTypeFromCbmgmtx(t cbmgmtx.BucketType) (admin_bucket_v1.BucketType, *status.Status) {
	switch t {
	case cbmgmtx.BucketTypeMemcached:
		return admin_bucket_v1.BucketType_MEMCACHED, nil
	case cbmgmtx.BucketTypeCouchbase:
		return admin_bucket_v1.BucketType_COUCHBASE, nil
	case cbmgmtx.BucketTypeEphemeral:
		return admin_bucket_v1.BucketType_EPHEMERAL, nil
	}

	return admin_bucket_v1.BucketType(0), status.New(codes.InvalidArgument, "invalid bucket type received")
}

func bucketTypeToCbmgmtx(t admin_bucket_v1.BucketType) (cbmgmtx.BucketType, *status.Status) {
	switch t {
	case admin_bucket_v1.BucketType_MEMCACHED:
		return cbmgmtx.BucketTypeMemcached, nil
	case admin_bucket_v1.BucketType_COUCHBASE:
		return cbmgmtx.BucketTypeCouchbase, nil
	case admin_bucket_v1.BucketType_EPHEMERAL:
		return cbmgmtx.BucketTypeEphemeral, nil
	}

	return cbmgmtx.BucketType(""), status.New(codes.InvalidArgument, "invalid bucket type specified")
}

func evictionModeFromCbmgmtx(em cbmgmtx.EvictionPolicyType) (admin_bucket_v1.EvictionMode, *status.Status) {
	switch em {
	case cbmgmtx.EvictionPolicyTypeFull:
		return admin_bucket_v1.EvictionMode_FULL, nil
	case cbmgmtx.EvictionPolicyTypeValueOnly:
		return admin_bucket_v1.EvictionMode_VALUE_ONLY, nil
	case cbmgmtx.EvictionPolicyTypeNotRecentlyUsed:
		return admin_bucket_v1.EvictionMode_NOT_RECENTLY_USED, nil
	case cbmgmtx.EvictionPolicyTypeNoEviction:
		return admin_bucket_v1.EvictionMode_NONE, nil
	}

	return admin_bucket_v1.EvictionMode(0), status.New(codes.InvalidArgument, "invalid eviction mode received")
}

func evictionModeToCbmgmtx(em admin_bucket_v1.EvictionMode) (cbmgmtx.EvictionPolicyType, *status.Status) {
	switch em {
	case admin_bucket_v1.EvictionMode_FULL:
		return cbmgmtx.EvictionPolicyTypeFull, nil
	case admin_bucket_v1.EvictionMode_VALUE_ONLY:
		return cbmgmtx.EvictionPolicyTypeValueOnly, nil
	case admin_bucket_v1.EvictionMode_NOT_RECENTLY_USED:
		return cbmgmtx.EvictionPolicyTypeNotRecentlyUsed, nil
	case admin_bucket_v1.EvictionMode_NONE:
		return cbmgmtx.EvictionPolicyTypeNoEviction, nil
	}

	return cbmgmtx.EvictionPolicyType(""), status.New(codes.InvalidArgument, "invalid eviction mode specified")
}

func compressionModeFromCbmgmtx(cm cbmgmtx.CompressionMode) (admin_bucket_v1.CompressionMode, *status.Status) {
	switch cm {
	case cbmgmtx.CompressionModeOff:
		return admin_bucket_v1.CompressionMode_OFF, nil
	case cbmgmtx.CompressionModePassive:
		return admin_bucket_v1.CompressionMode_PASSIVE, nil
	case cbmgmtx.CompressionModeActive:
		return admin_bucket_v1.CompressionMode_ACTIVE, nil

	}

	return admin_bucket_v1.CompressionMode(0), status.New(codes.InvalidArgument, "invalid compression mode received")
}

func compressionModeToCbmgmtx(cm admin_bucket_v1.CompressionMode) (cbmgmtx.CompressionMode, *status.Status) {
	switch cm {
	case admin_bucket_v1.CompressionMode_OFF:
		return cbmgmtx.CompressionModeOff, nil
	case admin_bucket_v1.CompressionMode_PASSIVE:
		return cbmgmtx.CompressionModePassive, nil
	case admin_bucket_v1.CompressionMode_ACTIVE:
		return cbmgmtx.CompressionModeActive, nil
	}

	return cbmgmtx.CompressionMode(""), status.New(codes.InvalidArgument, "invalid compression mode specified")
}

func storageBackendFromCbmgmtx(sb cbmgmtx.StorageBackend) (admin_bucket_v1.StorageBackend, *status.Status) {
	switch sb {
	case cbmgmtx.StorageBackendCouchstore:
		return admin_bucket_v1.StorageBackend_COUCHSTORE, nil
	case cbmgmtx.StorageBackendMagma:
		return admin_bucket_v1.StorageBackend_MAGMA, nil
	}

	return admin_bucket_v1.StorageBackend(0), status.New(codes.InvalidArgument, "invalid storage backend received")
}

func storageBackendToCbmgmtx(sb admin_bucket_v1.StorageBackend) (cbmgmtx.StorageBackend, *status.Status) {
	switch sb {
	case admin_bucket_v1.StorageBackend_COUCHSTORE:
		return cbmgmtx.StorageBackendCouchstore, nil
	case admin_bucket_v1.StorageBackend_MAGMA:
		return cbmgmtx.StorageBackendMagma, nil
	}

	return cbmgmtx.StorageBackend(""), status.New(codes.InvalidArgument, "invalid storage backend specified")
}

func conflictResolutionTypeFromCbmgmtx(t cbmgmtx.ConflictResolutionType) (admin_bucket_v1.ConflictResolutionType, *status.Status) {
	switch t {
	case cbmgmtx.ConflictResolutionTypeTimestamp:
		return admin_bucket_v1.ConflictResolutionType_TIMESTAMP, nil
	case cbmgmtx.ConflictResolutionTypeSequenceNumber:
		return admin_bucket_v1.ConflictResolutionType_SEQUENCE_NUMBER, nil
	case cbmgmtx.ConflictResolutionTypeCustom:
		return admin_bucket_v1.ConflictResolutionType_CUSTOM, nil
	}

	return admin_bucket_v1.ConflictResolutionType(0), status.New(codes.InvalidArgument, "invalid conflict resolution type received")
}

func conflictResolutionTypeToCbmgmtx(t admin_bucket_v1.ConflictResolutionType) (cbmgmtx.ConflictResolutionType, *status.Status) {
	switch t {
	case admin_bucket_v1.ConflictResolutionType_TIMESTAMP:
		return cbmgmtx.ConflictResolutionTypeTimestamp, nil
	case admin_bucket_v1.ConflictResolutionType_SEQUENCE_NUMBER:
		return cbmgmtx.ConflictResolutionTypeSequenceNumber, nil
	case admin_bucket_v1.ConflictResolutionType_CUSTOM:
		return cbmgmtx.ConflictResolutionTypeCustom, nil
	}

	return cbmgmtx.ConflictResolutionType(""), status.New(codes.InvalidArgument, "invalid conflict resolution type specified")
}
