package server_v1

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/stellar-gateway/gateway/apiversion"
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

func secsExpiryToGocbcorex(expiry uint32) uint32 {
	// If the duration is 0, that indicates never-expires
	if expiry == 0 {
		return 0
	}

	// If the duration is less than one second, we must force the
	// value to 1 to avoid accidentally making it never expire.
	if expiry < 1 {
		return 1
	}

	// If the duration is less than 30 days then send as seconds.
	if expiry <= 2592000 {
		return expiry
	}

	// Send the duration as a unix timestamp of now plus duration.
	return uint32(time.Now().Add(time.Duration(expiry) * time.Second).Unix())
}

func tokenFromGocbcorex(bucketName string, token gocbcorex.MutationToken) *kv_v1.MutationToken {
	return &kv_v1.MutationToken{
		BucketName:  bucketName,
		VbucketId:   uint32(token.VbID),
		VbucketUuid: token.VbUuid,
		SeqNo:       token.SeqNo,
	}
}

func durabilityLevelToMemdx(dl kv_v1.DurabilityLevel) (memdx.DurabilityLevel, *status.Status) {
	switch dl {
	case kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY:
		return memdx.DurabilityLevelMajority, nil
	case kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return memdx.DurabilityLevelMajorityAndPersistToActive, nil
	case kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
		return memdx.DurabilityLevelPersistToMajority, nil
	}

	// TODO(brett19): We should attach the field reference information here indicating
	// what specific field the user incorrectly specified.
	return memdx.DurabilityLevel(0), status.New(codes.InvalidArgument, "invalid durability level specified")
}

func durabilityLevelFromCbmgmtx(dl cbmgmtx.DurabilityLevel) (*kv_v1.DurabilityLevel, *status.Status) {
	switch dl {
	case cbmgmtx.DurabilityLevelUnset:
		return nil, nil
	case cbmgmtx.DurabilityLevelNone:
		return nil, nil
	case cbmgmtx.DurabilityLevelMajority:
		lvl := kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY
		return &lvl, nil
	case cbmgmtx.DurabilityLevelMajorityAndPersistOnMaster:
		lvl := kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE
		return &lvl, nil
	case cbmgmtx.DurabilityLevelPersistToMajority:
		lvl := kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY
		return &lvl, nil
	}

	return nil, status.New(codes.Internal, "invalid durability level received")
}

func durabilityLevelToCbmgmtx(dl kv_v1.DurabilityLevel) (cbmgmtx.DurabilityLevel, *status.Status) {
	switch dl {
	case kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY:
		return cbmgmtx.DurabilityLevelMajority, nil
	case kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return cbmgmtx.DurabilityLevelMajorityAndPersistOnMaster, nil
	case kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
		return cbmgmtx.DurabilityLevelPersistToMajority, nil
	}

	// TODO(brett19): We should attach the field reference information here indicating
	// what specific field the user incorrectly specified.
	return cbmgmtx.DurabilityLevel(""), status.New(codes.InvalidArgument, "invalid durability level specified")
}

func bucketTypeFromCbmgmtx(t cbmgmtx.BucketType) (admin_bucket_v1.BucketType, *status.Status) {
	switch t {
	case cbmgmtx.BucketTypeCouchbase:
		return admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE, nil
	case cbmgmtx.BucketTypeEphemeral:
		return admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL, nil
	}

	return admin_bucket_v1.BucketType(0), status.New(codes.Internal, "invalid bucket type received")
}

func bucketTypeToCbmgmtx(t admin_bucket_v1.BucketType) (cbmgmtx.BucketType, *status.Status) {
	switch t {
	case admin_bucket_v1.BucketType_BUCKET_TYPE_COUCHBASE:
		return cbmgmtx.BucketTypeCouchbase, nil
	case admin_bucket_v1.BucketType_BUCKET_TYPE_EPHEMERAL:
		return cbmgmtx.BucketTypeEphemeral, nil
	}

	return cbmgmtx.BucketType(""), status.New(codes.InvalidArgument, "invalid bucket type specified")
}

func evictionModeFromCbmgmtx(em cbmgmtx.EvictionPolicyType) (admin_bucket_v1.EvictionMode, *status.Status) {
	switch em {
	case cbmgmtx.EvictionPolicyTypeFull:
		return admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL, nil
	case cbmgmtx.EvictionPolicyTypeValueOnly:
		return admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY, nil
	case cbmgmtx.EvictionPolicyTypeNotRecentlyUsed:
		return admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED, nil
	case cbmgmtx.EvictionPolicyTypeNoEviction:
		return admin_bucket_v1.EvictionMode_EVICTION_MODE_NONE, nil
	}

	return admin_bucket_v1.EvictionMode(0), status.New(codes.Internal, "invalid eviction mode received")
}

func evictionModeToCbmgmtx(em admin_bucket_v1.EvictionMode) (cbmgmtx.EvictionPolicyType, *status.Status) {
	switch em {
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_FULL:
		return cbmgmtx.EvictionPolicyTypeFull, nil
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_VALUE_ONLY:
		return cbmgmtx.EvictionPolicyTypeValueOnly, nil
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_NOT_RECENTLY_USED:
		return cbmgmtx.EvictionPolicyTypeNotRecentlyUsed, nil
	case admin_bucket_v1.EvictionMode_EVICTION_MODE_NONE:
		return cbmgmtx.EvictionPolicyTypeNoEviction, nil
	}

	return cbmgmtx.EvictionPolicyType(""), status.New(codes.InvalidArgument, "invalid eviction mode specified")
}

func compressionModeFromCbmgmtx(cm cbmgmtx.CompressionMode) (admin_bucket_v1.CompressionMode, *status.Status) {
	switch cm {
	case cbmgmtx.CompressionModeOff:
		return admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF, nil
	case cbmgmtx.CompressionModePassive:
		return admin_bucket_v1.CompressionMode_COMPRESSION_MODE_PASSIVE, nil
	case cbmgmtx.CompressionModeActive:
		return admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE, nil

	}

	return admin_bucket_v1.CompressionMode(0), status.New(codes.Internal, "invalid compression mode received")
}

func compressionModeToCbmgmtx(cm admin_bucket_v1.CompressionMode) (cbmgmtx.CompressionMode, *status.Status) {
	switch cm {
	case admin_bucket_v1.CompressionMode_COMPRESSION_MODE_OFF:
		return cbmgmtx.CompressionModeOff, nil
	case admin_bucket_v1.CompressionMode_COMPRESSION_MODE_PASSIVE:
		return cbmgmtx.CompressionModePassive, nil
	case admin_bucket_v1.CompressionMode_COMPRESSION_MODE_ACTIVE:
		return cbmgmtx.CompressionModeActive, nil
	}

	return cbmgmtx.CompressionMode(""), status.New(codes.InvalidArgument, "invalid compression mode specified")
}

func storageBackendFromCbmgmtx(sb cbmgmtx.StorageBackend) (*admin_bucket_v1.StorageBackend, *status.Status) {
	switch sb {
	case cbmgmtx.StorageBackendCouchstore:
		backend := admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE
		return &backend, nil
	case cbmgmtx.StorageBackendMagma:
		backend := admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA
		return &backend, nil
	case cbmgmtx.StorageBackendUnset:
		return nil, nil
	case "undefined":
		// "undefined is returned by some server versions for memcached buckets.
		return nil, nil
	}

	return nil, status.New(codes.Internal, "invalid storage backend received")
}

func storageBackendToCbmgmtx(sb admin_bucket_v1.StorageBackend) (cbmgmtx.StorageBackend, *status.Status) {
	switch sb {
	case admin_bucket_v1.StorageBackend_STORAGE_BACKEND_COUCHSTORE:
		return cbmgmtx.StorageBackendCouchstore, nil
	case admin_bucket_v1.StorageBackend_STORAGE_BACKEND_MAGMA:
		return cbmgmtx.StorageBackendMagma, nil
	}

	return cbmgmtx.StorageBackend(""), status.New(codes.InvalidArgument, "invalid storage backend specified")
}

func conflictResolutionTypeFromCbmgmtx(t cbmgmtx.ConflictResolutionType) (admin_bucket_v1.ConflictResolutionType, *status.Status) {
	switch t {
	case cbmgmtx.ConflictResolutionTypeTimestamp:
		return admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP, nil
	case cbmgmtx.ConflictResolutionTypeSequenceNumber:
		return admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER, nil
	case cbmgmtx.ConflictResolutionTypeCustom:
		return admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_CUSTOM, nil
	}

	return admin_bucket_v1.ConflictResolutionType(0), status.New(codes.Internal, "invalid conflict resolution type received")
}

func conflictResolutionTypeToCbmgmtx(t admin_bucket_v1.ConflictResolutionType) (cbmgmtx.ConflictResolutionType, *status.Status) {
	switch t {
	case admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP:
		return cbmgmtx.ConflictResolutionTypeTimestamp, nil
	case admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER:
		return cbmgmtx.ConflictResolutionTypeSequenceNumber, nil
	case admin_bucket_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_CUSTOM:
		return cbmgmtx.ConflictResolutionTypeCustom, nil
	}

	return cbmgmtx.ConflictResolutionType(""), status.New(codes.InvalidArgument, "invalid conflict resolution type specified")
}

func indexStateFromQueryTableString(s string) (admin_query_v1.IndexState, *status.Status) {
	switch s {
	case "deferred":
		return admin_query_v1.IndexState_INDEX_STATE_DEFERRED, nil
	case "building":
		return admin_query_v1.IndexState_INDEX_STATE_BUILDING, nil
	case "pending":
		return admin_query_v1.IndexState_INDEX_STATE_PENDING, nil
	case "online":
		return admin_query_v1.IndexState_INDEX_STATE_ONLINE, nil
	case "offline":
		return admin_query_v1.IndexState_INDEX_STATE_OFFLINE, nil
	case "abridged":
		return admin_query_v1.IndexState_INDEX_STATE_ABRIDGED, nil
	case "scheduled":
		return admin_query_v1.IndexState_INDEX_STATE_SCHEDULED, nil
	}

	return admin_query_v1.IndexState(0), status.New(codes.Internal, "invalid index state specified")
}

func scanConsistencyToCbqueryx(t query_v1.QueryRequest_ScanConsistency) (cbqueryx.ScanConsistency, *status.Status) {
	switch t {
	case query_v1.QueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED:
		return cbqueryx.ScanConsistencyNotBounded, nil
	case query_v1.QueryRequest_SCAN_CONSISTENCY_REQUEST_PLUS:
		return cbqueryx.ScanConsistencyRequestPlus, nil
	}

	return cbqueryx.ScanConsistency(""), status.New(codes.InvalidArgument, "invalid scan consistency option specified")
}

func durabilityLevelToCbqueryx(t query_v1.QueryRequest_DurabilityLevel) (cbqueryx.DurabilityLevel, *status.Status) {
	switch t {
	case query_v1.QueryRequest_DURABILITY_LEVEL_NONE:
		return cbqueryx.DurabilityLevelNone, nil
	case query_v1.QueryRequest_DURABILITY_LEVEL_MAJORITY:
		return cbqueryx.DurabilityLevelMajority, nil
	case query_v1.QueryRequest_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return cbqueryx.DurabilityLevelMajorityAndPersistActive, nil
	case query_v1.QueryRequest_DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
		return cbqueryx.DurabilityLevelPersistToMajority, nil
	}

	return cbqueryx.DurabilityLevel(""), status.New(codes.InvalidArgument, "invalid durability level specified")
}

func checkApiVersion(ctx context.Context, requiredVersion uint64, featureName string) *status.Status {
	apiVersion, err := apiversion.GetAPIVersion(ctx)
	if err != nil {
		return status.FromContextError(err)
	}

	if apiVersion < requiredVersion {
		return status.New(codes.Unimplemented,
			fmt.Sprintf("Feature (%s) is not available in the requested API version", featureName))
	}

	return nil
}
