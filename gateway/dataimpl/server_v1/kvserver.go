/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServer struct {
	kv_v1.UnimplementedKvServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewKvServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *KvServer {
	return &KvServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *KvServer) Get(ctx context.Context, in *kv_v1.GetRequest) (*kv_v1.GetResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetExOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.WithExpiry = true
	opts.WithFlags = true
	opts.Project = in.Project

	result, err := bucketAgent.GetEx(ctx, &opts)
	if err != nil {
		var pathErr *gocbcorex.PathProjectionError
		var path string
		if errors.As(err, &pathErr) {
			path = pathErr.Path
		}

		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrSubDocNotJSON) {
			return nil, s.errorHandler.NewSdDocNotJsonStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrSubDocPathInvalid) {
			return nil, s.errorHandler.NewSdPathInvalidStatus(err, path).Err()
		} else if errors.Is(err, memdx.ErrSubDocPathMismatch) {
			return nil, s.errorHandler.NewSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, path).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var expiryTime time.Time
	if result.Expiry != 0 {
		expiryTime = time.Unix(int64(result.Expiry), 0)
	}

	resp := &kv_v1.GetResponse{
		ContentFlags: uint32(result.Flags),
		Cas:          result.Cas,
		Expiry:       timeFromGo(expiryTime),
	}

	isCompressed, respValue, errSt :=
		CompressHandler{}.MaybeCompressContent(result.Value, 0, in.Compression)
	if errSt != nil {
		return nil, errSt.Err()
	}
	if isCompressed {
		resp.Content = &kv_v1.GetResponse_ContentCompressed{
			ContentCompressed: respValue,
		}
	} else {
		resp.Content = &kv_v1.GetResponse_ContentUncompressed{
			ContentUncompressed: respValue,
		}
	}

	return resp, nil
}

func (s *KvServer) GetAndTouch(ctx context.Context, in *kv_v1.GetAndTouchRequest) (*kv_v1.GetAndTouchResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetAndTouchOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	switch expirySpec := in.Expiry.(type) {
	case *kv_v1.GetAndTouchRequest_ExpiryTime:
		opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
	case *kv_v1.GetAndTouchRequest_ExpirySecs:
		opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
	default:
		return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
	}

	result, err := bucketAgent.GetAndTouch(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resp := &kv_v1.GetAndTouchResponse{
		ContentFlags: result.Flags,
		Cas:          result.Cas,
	}

	isCompressed, respValue, errSt :=
		CompressHandler{}.MaybeCompressContent(result.Value, result.Datatype, in.Compression)
	if errSt != nil {
		return nil, errSt.Err()
	}
	if isCompressed {
		resp.Content = &kv_v1.GetAndTouchResponse_ContentCompressed{
			ContentCompressed: respValue,
		}
	} else {
		resp.Content = &kv_v1.GetAndTouchResponse_ContentUncompressed{
			ContentUncompressed: respValue,
		}
	}

	return resp, nil
}

func (s *KvServer) GetAndLock(ctx context.Context, in *kv_v1.GetAndLockRequest) (*kv_v1.GetAndLockResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetAndLockOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.LockTime = in.LockTime

	result, err := bucketAgent.GetAndLock(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resp := &kv_v1.GetAndLockResponse{
		ContentFlags: result.Flags,
		Cas:          result.Cas,
	}

	isCompressed, respValue, errSt :=
		CompressHandler{}.MaybeCompressContent(result.Value, result.Datatype, in.Compression)
	if errSt != nil {
		return nil, errSt.Err()
	}
	if isCompressed {
		resp.Content = &kv_v1.GetAndLockResponse_ContentCompressed{
			ContentCompressed: respValue,
		}
	} else {
		resp.Content = &kv_v1.GetAndLockResponse_ContentUncompressed{
			ContentUncompressed: respValue,
		}
	}

	return resp, nil
}

func (s *KvServer) Unlock(ctx context.Context, in *kv_v1.UnlockRequest) (*kv_v1.UnlockResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(&in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.UnlockOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Cas = in.Cas

	_, err := bucketAgent.Unlock(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotLocked) {
			return nil, s.errorHandler.NewDocNotLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.UnlockResponse{}, nil
}

func (s *KvServer) Touch(ctx context.Context, in *kv_v1.TouchRequest) (*kv_v1.TouchResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.TouchOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	switch expirySpec := in.Expiry.(type) {
	case *kv_v1.TouchRequest_ExpiryTime:
		opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
	case *kv_v1.TouchRequest_ExpirySecs:
		opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
	default:
		return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
	}

	result, err := bucketAgent.Touch(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.TouchResponse{
		Cas: result.Cas,
	}, nil
}

func (s *KvServer) Insert(ctx context.Context, in *kv_v1.InsertRequest) (*kv_v1.InsertResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.AddOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Flags = in.ContentFlags

	switch content := in.Content.(type) {
	case *kv_v1.InsertRequest_ContentUncompressed:
		opts.Value = content.ContentUncompressed
	case *kv_v1.InsertRequest_ContentCompressed:
		opts.Value = content.ContentCompressed
		opts.Datatype = opts.Datatype | memdx.DatatypeFlagCompressed
	default:
		return nil, status.New(codes.InvalidArgument, "CompressedContent or UncompressedContent must be specified.").Err()
	}

	if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.InsertRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.InsertRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Add(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, false).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.InsertResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Exists(ctx context.Context, in *kv_v1.ExistsRequest) (*kv_v1.ExistsResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetMetaOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	result, err := bucketAgent.GetMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			// Exists returns false rather than an error if a document is not found.
			return &kv_v1.ExistsResponse{
				Result: false,
			}, nil
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	if result.Deleted {
		return &kv_v1.ExistsResponse{
			Result: false,
		}, nil
	}

	return &kv_v1.ExistsResponse{
		Result: true,
		Cas:    result.Cas,
	}, nil
}

func (s *KvServer) Upsert(ctx context.Context, in *kv_v1.UpsertRequest) (*kv_v1.UpsertResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.UpsertOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Flags = in.ContentFlags

	switch content := in.Content.(type) {
	case *kv_v1.UpsertRequest_ContentUncompressed:
		opts.Value = content.ContentUncompressed
	case *kv_v1.UpsertRequest_ContentCompressed:
		opts.Value = content.ContentCompressed
		opts.Datatype = opts.Datatype | memdx.DatatypeFlagCompressed
	default:
		return nil, status.New(codes.InvalidArgument, "CompressedContent or UncompressedContent must be specified.").Err()
	}

	if in.Expiry == nil {
		if in.PreserveExpiryOnExisting != nil && *in.PreserveExpiryOnExisting {
			return nil, status.New(codes.InvalidArgument,
				"Cannot specify preserve expiry with no expiry, leave expiry undefined to preserve expiry.").Err()
		}

		opts.PreserveExpiry = true
	} else {
		if in.PreserveExpiryOnExisting != nil && *in.PreserveExpiryOnExisting {
			opts.PreserveExpiry = true
		}

		if expirySpec, ok := in.Expiry.(*kv_v1.UpsertRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.UpsertRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}

		if opts.PreserveExpiry && opts.Expiry == 0 {
			return nil, status.New(codes.InvalidArgument,
				"Cannot specify preserve expiry with zero expiry, leave expiry undefined to preserve expiry.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Upsert(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, false).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.UpsertResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Replace(ctx context.Context, in *kv_v1.ReplaceRequest) (*kv_v1.ReplaceResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.ReplaceOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Flags = in.ContentFlags

	switch content := in.Content.(type) {
	case *kv_v1.ReplaceRequest_ContentUncompressed:
		opts.Value = content.ContentUncompressed
	case *kv_v1.ReplaceRequest_ContentCompressed:
		opts.Value = content.ContentCompressed
		opts.Datatype = opts.Datatype | memdx.DatatypeFlagCompressed
	default:
		return nil, status.New(codes.InvalidArgument, "CompressedContent or UncompressedContent must be specified.").Err()
	}

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.ReplaceRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.ReplaceRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Replace(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, false).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.ReplaceResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Remove(ctx context.Context, in *kv_v1.RemoveRequest) (*kv_v1.RemoveResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.DeleteOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Delete(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.RemoveResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Increment(ctx context.Context, in *kv_v1.IncrementRequest) (*kv_v1.IncrementResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.IncrementOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Delta = in.Delta

	if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.IncrementRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.IncrementRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	if in.Initial != nil {
		opts.Initial = uint64(*in.Initial)
	} else {
		opts.Initial = uint64(0xFFFFFFFFFFFFFFFF)
	}

	result, err := bucketAgent.Increment(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDeltaBadval) {
			return nil, s.errorHandler.NewDocNotNumericStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.IncrementResponse{
		Cas:           result.Cas,
		Content:       int64(result.Value),
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Decrement(ctx context.Context, in *kv_v1.DecrementRequest) (*kv_v1.DecrementResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.DecrementOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Delta = in.Delta

	if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.DecrementRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.DecrementRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	if in.Initial != nil {
		opts.Initial = uint64(*in.Initial)
	} else {
		opts.Initial = uint64(0xFFFFFFFFFFFFFFFF)
	}

	result, err := bucketAgent.Decrement(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDeltaBadval) {
			return nil, s.errorHandler.NewDocNotNumericStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.DecrementResponse{
		Cas:           result.Cas,
		Content:       int64(result.Value),
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Append(ctx context.Context, in *kv_v1.AppendRequest) (*kv_v1.AppendResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.AppendOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = in.Content

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Append(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, true).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.AppendResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Prepend(ctx context.Context, in *kv_v1.PrependRequest) (*kv_v1.PrependResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.PrependOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = in.Content

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Prepend(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, true).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &kv_v1.PrependResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) LookupIn(ctx context.Context, in *kv_v1.LookupInRequest) (*kv_v1.LookupInResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.LookupInOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	if len(in.Specs) == 0 {
		return nil, status.New(codes.InvalidArgument, "at least one lookup spec must be specified").Err()
	}

	ops := make([]memdx.LookupInOp, len(in.Specs))
	for i, spec := range in.Specs {
		if len(spec.Path) > 1024 {
			return nil, status.New(codes.InvalidArgument, "sub-document paths must be less than 1024 characters long").Err()
		}

		var op memdx.LookupInOpType
		switch spec.Operation {
		case kv_v1.LookupInRequest_Spec_OPERATION_GET:
			if spec.Path == "" {
				op = memdx.LookupInOpTypeGetDoc
			} else {
				op = memdx.LookupInOpTypeGet
			}
		case kv_v1.LookupInRequest_Spec_OPERATION_COUNT:
			op = memdx.LookupInOpTypeGetCount
		case kv_v1.LookupInRequest_Spec_OPERATION_EXISTS:
			op = memdx.LookupInOpTypeExists
		default:
			return nil, status.New(codes.InvalidArgument, "invalid lookup in op type specified").Err()
		}
		var flags memdx.SubdocOpFlag
		if spec.Flags != nil {
			if spec.Flags.GetXattr() {
				flags = memdx.SubdocOpFlagXattrPath
			}
		}
		ops[i] = memdx.LookupInOp{
			Op:    op,
			Flags: flags,
			Path:  []byte(spec.Path),
		}
	}
	reordered, indexes := memdx.ReorderSubdocOps(ops)
	opts.Ops = reordered

	if in.Flags != nil {
		if in.Flags.GetAccessDeleted() {
			opts.Flags = opts.Flags | memdx.SubdocDocFlagAccessDeleted
		}
	}

	result, err := bucketAgent.LookupIn(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrSubDocInvalidCombo) {
			return nil, s.errorHandler.NewSdBadCombo(err).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resultSpecs := make([]*kv_v1.LookupInResponse_Spec, len(result.Ops))
	for i, op := range result.Ops {
		spec := &kv_v1.LookupInResponse_Spec{
			Content: op.Value,
		}
		if op.Err != nil {
			if errors.Is(op.Err, memdx.ErrSubDocDocTooDeep) {
				spec.Status = s.errorHandler.NewSdDocTooDeepStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocNotJSON) {
				spec.Status = s.errorHandler.NewSdDocNotJsonStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
				spec.Status = s.errorHandler.NewSdPathNotFoundStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[i].Path).Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocPathInvalid) {
				spec.Status = s.errorHandler.NewSdPathInvalidStatus(op.Err, in.Specs[i].Path).Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocPathMismatch) {
				spec.Status = s.errorHandler.NewSdPathMismatchStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[i].Path).Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocPathTooBig) {
				spec.Status = s.errorHandler.NewSdPathTooBigStatus(op.Err, in.Specs[i].Path).Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocXattrUnknownVAttr) {
				spec.Status = s.errorHandler.NewSdXattrUnknownVattrStatus(op.Err, in.Specs[i].Path).Proto()
			} else {
				spec.Status = s.errorHandler.NewGenericStatus(op.Err).Proto()
			}
		}

		// BUG(protostellar#23): This is implemented on top of the standard KV protocol.
		if in.Specs[i].Operation == kv_v1.LookupInRequest_Spec_OPERATION_EXISTS {
			if op.Err == nil {
				spec.Status = nil
				spec.Content = []byte("true")
			} else if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
				spec.Status = nil
				spec.Content = []byte("false")
			}
		}

		index := indexes[i]
		resultSpecs[index] = spec
	}

	return &kv_v1.LookupInResponse{
		Cas:   result.Cas,
		Specs: resultSpecs,
	}, nil
}

func (s *KvServer) MutateIn(ctx context.Context, in *kv_v1.MutateInRequest) (*kv_v1.MutateInResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.MutateInOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	if len(in.Specs) == 0 {
		return nil, status.New(codes.InvalidArgument, "at least one mutation spec must be specified").Err()
	}

	ops := make([]memdx.MutateInOp, len(in.Specs))
	for i, spec := range in.Specs {
		if len(spec.Path) > 1024 {
			return nil, status.New(codes.InvalidArgument, "sub-document paths must be less than 1024 characters long").Err()
		}

		var op memdx.MutateInOpType
		switch spec.Operation {
		case kv_v1.MutateInRequest_Spec_OPERATION_UPSERT:
			op = memdx.MutateInOpTypeDictSet
		case kv_v1.MutateInRequest_Spec_OPERATION_REPLACE:
			if spec.Path == "" {
				op = memdx.MutateInOpTypeSetDoc
			} else {
				op = memdx.MutateInOpTypeReplace
			}
		case kv_v1.MutateInRequest_Spec_OPERATION_REMOVE:
			if spec.Path == "" {
				op = memdx.MutateInOpTypeDeleteDoc
			} else {
				op = memdx.MutateInOpTypeDelete
			}
		case kv_v1.MutateInRequest_Spec_OPERATION_INSERT:
			op = memdx.MutateInOpTypeDictAdd
		case kv_v1.MutateInRequest_Spec_OPERATION_COUNTER:
			op = memdx.MutateInOpTypeCounter
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND:
			op = memdx.MutateInOpTypeArrayPushLast
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE:
			op = memdx.MutateInOpTypeArrayAddUnique
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_INSERT:
			op = memdx.MutateInOpTypeArrayInsert
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_PREPEND:
			op = memdx.MutateInOpTypeArrayPushFirst
		default:
			return nil, status.New(codes.InvalidArgument, "invalid mutate in op type specified").Err()
		}
		var flags memdx.SubdocOpFlag
		if spec.Flags != nil {
			if spec.Flags.GetXattr() {
				flags |= memdx.SubdocOpFlagXattrPath
			}
			if spec.Flags.GetCreatePath() {
				flags |= memdx.SubdocOpFlagMkDirP
			}
		}
		ops[i] = memdx.MutateInOp{
			Op:    op,
			Flags: flags,
			Path:  []byte(spec.Path),
			Value: spec.Content,
		}
	}
	reordered, indexes := memdx.ReorderSubdocOps(ops)
	opts.Ops = reordered

	if in.Flags != nil {
		if in.Flags.GetAccessDeleted() {
			opts.Flags = opts.Flags | memdx.SubdocDocFlagAccessDeleted
		}
	}

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.MutateInRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.MutateInRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	if in.StoreSemantic != nil {
		switch *in.StoreSemantic {
		case kv_v1.MutateInRequest_STORE_SEMANTIC_REPLACE:
			// This is just the default behaviour
		case kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT:
			opts.Flags = opts.Flags | memdx.SubdocDocFlagMkDoc
		case kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT:
			opts.Flags = opts.Flags | memdx.SubdocDocFlagAddDoc
		}
	}

	result, err := bucketAgent.MutateIn(ctx, &opts)
	if err != nil {
		/*
			There are additional errors that can appear here, but they are not included in
			our error handling logic since they should be caught by STG ahead of sending
			the invalid request to the server, or we don't expose the feature:
			- ErrSubDocInvalidCombo
			- ErrSubDocInvalidXattrOrder
			- ErrSubDocXattrInvalidKeyCombo
			- ErrSubDocXattrInvalidFlagCombo
			- ErrSubDocXattrUnknownMacro
			- ErrSubDocXattrUnknownVattrMacro
			- ErrSubDocXattrCannotModifyVAttr
			- ErrSubDocCanOnlyReviveDeletedDocuments
			- ErrSubDocDeletedDocumentCantHaveValue
		*/

		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			if in.StoreSemantic != nil && *in.StoreSemantic == kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT {
				return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, false).Err()
			}
			// We have no way to differentiate whether the document already existed here, so just treat it as expanding.
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, true).Err()
		} else if errors.Is(err, memdx.ErrSubDocInvalidCombo) {
			return nil, s.errorHandler.NewSdBadCombo(err).Err()
		} else if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else {
			var subdocErr *memdx.SubDocError
			if errors.As(err, &subdocErr) {
				if subdocErr.OpIndex >= len(in.Specs) {
					return nil, status.New(codes.Internal, "server responded with error opIndex outside of range of provided specs").Err()
				}

				if errors.Is(err, memdx.ErrSubDocDocTooDeep) {
					return nil, s.errorHandler.NewSdDocTooDeepStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
				} else if errors.Is(err, memdx.ErrSubDocNotJSON) {
					return nil, s.errorHandler.NewSdDocNotJsonStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathNotFound) {
					return nil, s.errorHandler.NewSdPathNotFoundStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathExists) {
					return nil, s.errorHandler.NewSdPathExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathInvalid) {
					return nil, s.errorHandler.NewSdPathInvalidStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathMismatch) {
					return nil, s.errorHandler.NewSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathTooBig) {
					return nil, s.errorHandler.NewSdPathTooBigStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocCantInsert) {
					opSpec := in.Specs[subdocErr.OpIndex]
					if opSpec.Operation == kv_v1.MutateInRequest_Spec_OPERATION_COUNTER {
						return nil, s.errorHandler.NewSdValueOutOfRangeStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
					} else {
						return nil, s.errorHandler.NewSdBadValueStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
					}
				} else if errors.Is(err, memdx.ErrSubDocBadRange) {
					return nil, s.errorHandler.NewSdBadRangeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocBadDelta) {
					return nil, s.errorHandler.NewSdBadDeltaStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocValueTooDeep) {
					return nil, s.errorHandler.NewSdValueTooDeepStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
				}
			}
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resultSpecs := make([]*kv_v1.MutateInResponse_Spec, len(result.Ops))
	for i, op := range result.Ops {
		spec := &kv_v1.MutateInResponse_Spec{
			Content: op.Value,
		}

		index := indexes[i]
		resultSpecs[index] = spec
	}

	return &kv_v1.MutateInResponse{
		Cas:           result.Cas,
		Specs:         resultSpecs,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) GetAllReplicas(in *kv_v1.GetAllReplicasRequest, out kv_v1.KvService_GetAllReplicasServer) error {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(out.Context(), in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return errSt.Err()
	}

	var opts gocbcorex.GetAllReplicasOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.BucketName = in.BucketName
	replicaStream, err := bucketAgent.GetAllReplicas(out.Context(), &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else {
			return s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	for {
		res, err := replicaStream.Next()
		if err != nil {
			s.logger.Debug("error reading a replica", zap.Error(err))
			return err
		}

		if res == nil {
			break
		}

		err = out.Send(&kv_v1.GetAllReplicasResponse{
			IsReplica:    res.IsReplica,
			Content:      res.Value,
			ContentFlags: res.Flags,
			Cas:          res.Cas})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *KvServer) checkKey(key string) *status.Status {
	if len(key) > 250 || len(key) < 1 {
		return s.errorHandler.NewInvalidKeyLengthStatus(key)
	}

	return nil
}

func (s *KvServer) checkCAS(cas *uint64) *status.Status {
	if cas != nil && *cas == 0 {
		return s.errorHandler.NewZeroCasStatus()
	}

	return nil
}
