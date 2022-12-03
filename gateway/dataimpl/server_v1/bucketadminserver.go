package server_v1

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/genproto/admin_bucket_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BucketAdminServer struct {
	admin_bucket_v1.UnimplementedBucketAdminServer

	cbClient *gocb.Cluster
}

func (s *BucketAdminServer) ListBuckets(ctx context.Context, req *admin_bucket_v1.ListBucketsRequest) (*admin_bucket_v1.ListBucketsResponse, error) {
	bktMgr := s.cbClient.Buckets()

	result, err := bktMgr.GetAllBuckets(&gocb.GetAllBucketsOptions{})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	var buckets []*admin_bucket_v1.ListBucketsResponse_Bucket
	for bucketName, bucket := range result {
		bucketType, errSt := bucketTypeFromGocb(bucket.BucketType)
		if errSt != nil {
			return nil, errSt.Err()
		}

		evictionMode, errSt := evictionModeFromGocb(bucket.EvictionPolicy)
		if errSt != nil {
			return nil, errSt.Err()
		}

		compressionMode, errSt := compressionModeFromGocb(bucket.CompressionMode)
		if errSt != nil {
			return nil, errSt.Err()
		}

		minimumDurabilityLevel, errSt := durabilityLevelFromGocb(bucket.MinimumDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}

		storageBackend, errSt := storageBackendFromGocb(bucket.StorageBackend)
		if errSt != nil {
			return nil, errSt.Err()
		}

		// TODO(brett19): Fix conflict resolution type in list buckets to return the proper value once gocb is fixed.
		conflictResolutionType, errSt := conflictResolutionTypeFromGocb(gocb.ConflictResolutionTypeTimestamp)
		if errSt != nil {
			return nil, errSt.Err()
		}

		buckets = append(buckets, &admin_bucket_v1.ListBucketsResponse_Bucket{
			BucketName:             bucketName,
			FlushEnabled:           bucket.FlushEnabled,
			RamQuotaBytes:          bucket.RAMQuotaMB * 1024 * 1024,
			NumReplicas:            bucket.NumReplicas,
			ReplicaIndexes:         !bucket.ReplicaIndexDisabled,
			BucketType:             bucketType,
			EvictionMode:           evictionMode,
			MaxExpirySecs:          uint32(bucket.MaxExpiry / time.Second),
			CompressionMode:        compressionMode,
			MinimumDurabilityLevel: minimumDurabilityLevel,
			StorageBackend:         storageBackend,
			ConflictResolutionType: conflictResolutionType,
		})
	}

	return &admin_bucket_v1.ListBucketsResponse{
		Buckets: buckets,
	}, nil
}

func (s *BucketAdminServer) CreateBucket(ctx context.Context, req *admin_bucket_v1.CreateBucketRequest) (*admin_bucket_v1.CreateBucketResponse, error) {
	bktMgr := s.cbClient.Buckets()

	flushEnabled := false
	if req.FlushEnabled != nil {
		flushEnabled = *req.FlushEnabled
	}

	replicaIndexes := false
	if req.ReplicaIndexes != nil {
		replicaIndexes = *req.ReplicaIndexes
	}

	bucketType, errSt := bucketTypeToGocb(req.BucketType)
	if errSt != nil {
		return nil, errSt.Err()
	}

	// TODO(brett19): Figure out how to properly handle default eviction type
	evictionPolicy := gocb.EvictionPolicyType("")
	if req.EvictionMode != nil {
		evictionPolicy, errSt = evictionModeToGocb(*req.EvictionMode)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	maxExpiry := 0 * time.Second
	if req.MaxExpirySecs != nil {
		maxExpiry = (time.Duration)(*req.MaxExpirySecs) * time.Second
	}

	compressionMode := gocb.CompressionModePassive
	if req.CompressionMode != nil {
		compressionMode, errSt = compressionModeToGocb(*req.CompressionMode)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	minimumDurabilityLevel := gocb.DurabilityLevelNone
	if req.MinimumDurabilityLevel != nil {
		minimumDurabilityLevel, errSt = durabilityLevelToGocb(*req.MinimumDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	storageBackend := gocb.StorageBackendCouchstore
	if req.StorageBackend != nil {
		storageBackend, errSt = storageBackendToGocb(*req.StorageBackend)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	conflictResolutionType, errSt := conflictResolutionTypeToGocb(*req.ConflictResolutionType)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := bktMgr.CreateBucket(gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:                 req.BucketName,
			FlushEnabled:         flushEnabled,
			ReplicaIndexDisabled: replicaIndexes,
			// TODO(brett19): Fix this once we can properly specify the size here...
			RAMQuotaMB:             req.RamQuotaBytes / 1024 / 1024,
			NumReplicas:            req.NumReplicas,
			BucketType:             bucketType,
			EvictionPolicy:         evictionPolicy,
			MaxExpiry:              maxExpiry,
			CompressionMode:        compressionMode,
			MinimumDurabilityLevel: minimumDurabilityLevel,
			StorageBackend:         storageBackend,
		},
		ConflictResolutionType: conflictResolutionType,
	}, nil)
	if err != nil {
		if errors.Is(err, gocb.ErrBucketExists) {
			return nil, newBucketExistsStatus(err, req.BucketName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_bucket_v1.CreateBucketResponse{}, nil
}

func (s *BucketAdminServer) UpdateBucket(ctx context.Context, req *admin_bucket_v1.UpdateBucketRequest) (*admin_bucket_v1.UpdateBucketResponse, error) {
	bktMgr := s.cbClient.Buckets()

	bucket, err := bktMgr.GetBucket(req.BucketName, nil)
	if err != nil {
		if errors.Is(err, gocb.ErrBucketNotFound) {
			return nil, newBucketMissingStatus(err, req.BucketName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	newBucket := *bucket

	var errSt *status.Status

	if req.FlushEnabled != nil {
		newBucket.FlushEnabled = *req.FlushEnabled
	}

	if req.RamQuotaBytes != nil {
		newBucket.RAMQuotaMB = *req.RamQuotaBytes / 1024 / 1024
	}

	if req.NumReplicas != nil {
		newBucket.NumReplicas = *req.NumReplicas
	}

	if req.ReplicaIndexes != nil {
		newBucket.ReplicaIndexDisabled = !*req.ReplicaIndexes
	}

	if req.EvictionMode != nil {
		newBucket.EvictionPolicy, errSt = evictionModeToGocb(*req.EvictionMode)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	if req.MaxExpirySecs != nil {
		newBucket.MaxExpiry = time.Duration(*req.MaxExpirySecs) * time.Second
	}

	if req.CompressionMode != nil {
		newBucket.CompressionMode, errSt = compressionModeToGocb(*req.CompressionMode)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	if req.MinimumDurabilityLevel != nil {
		newBucket.MinimumDurabilityLevel, errSt = durabilityLevelToGocb(*req.MinimumDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
	}

	if req.ConflictResolutionType != nil {
		// TODO(brett19): Implement correct handling of conflict resolution type when gocb bug is fixed.
		return nil, status.Errorf(codes.Unimplemented, "conflict resolution type updates are not implemented")
	}

	err = bktMgr.UpdateBucket(newBucket, nil)
	if err != nil {
		if errors.Is(err, gocb.ErrBucketNotFound) {
			return nil, newBucketMissingStatus(err, req.BucketName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_bucket_v1.UpdateBucketResponse{}, nil
}

func (s *BucketAdminServer) DeleteBucket(ctx context.Context, req *admin_bucket_v1.DeleteBucketRequest) (*admin_bucket_v1.DeleteBucketResponse, error) {
	bktMgr := s.cbClient.Buckets()

	err := bktMgr.DropBucket(req.BucketName, nil)
	if err != nil {
		if errors.Is(err, gocb.ErrBucketNotFound) {
			return nil, newBucketMissingStatus(err, req.BucketName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_bucket_v1.DeleteBucketResponse{}, nil
}

func NewBucketAdminServer(cbClient *gocb.Cluster) *BucketAdminServer {
	return &BucketAdminServer{
		cbClient: cbClient,
	}
}
