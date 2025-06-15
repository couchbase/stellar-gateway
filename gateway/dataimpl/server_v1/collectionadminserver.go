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
	"strconv"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/couchbase/stellar-gateway/gateway/apiversion"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

type CollectionAdminServer struct {
	admin_collection_v1.UnimplementedCollectionAdminServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewCollectionAdminServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *CollectionAdminServer {
	return &CollectionAdminServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *CollectionAdminServer) ListCollections(
	ctx context.Context,
	in *admin_collection_v1.ListCollectionsRequest,
) (*admin_collection_v1.ListCollectionsResponse, error) {
	apiVersion, err := apiversion.FromContext(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}

	var noExpirySupported bool
	if apiVersion >= apiversion.CollectionNoExpiry {
		noExpirySupported = true
	}

	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	result, err := bucketAgent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		OnBehalfOf: oboInfo,
		BucketName: in.BucketName,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var scopes []*admin_collection_v1.ListCollectionsResponse_Scope
	for _, scope := range result.Scopes {
		var collections []*admin_collection_v1.ListCollectionsResponse_Collection

		for _, collection := range scope.Collections {
			collectionSpec := &admin_collection_v1.ListCollectionsResponse_Collection{
				Name: collection.Name,
			}
			if collection.MaxTTL > 0 {
				collectionSpec.MaxExpirySecs = ptr.To(uint32(collection.MaxTTL))
			} else if collection.MaxTTL < 0 && noExpirySupported {
				// Since protostellar does not support negative values for MaxExpirySecs no_expiry and bucket expiry inheritance
				// are indicated with MaxExpirySecs = 0, and MaxExpirySecs = nil respectively
				collectionSpec.MaxExpirySecs = ptr.To(uint32(0))
			}
			collections = append(collections, collectionSpec)
		}

		scopes = append(scopes, &admin_collection_v1.ListCollectionsResponse_Scope{
			Name:        scope.Name,
			Collections: collections,
		})
	}

	return &admin_collection_v1.ListCollectionsResponse{
		Scopes: scopes,
	}, nil
}

func (s *CollectionAdminServer) CreateScope(
	ctx context.Context,
	in *admin_collection_v1.CreateScopeRequest,
) (*admin_collection_v1.CreateScopeResponse, error) {
	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	resp, err := bucketAgent.CreateScope(ctx, &cbmgmtx.CreateScopeOptions{
		OnBehalfOf: oboInfo,
		BucketName: in.BucketName,
		ScopeName:  in.ScopeName,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		} else if errors.Is(err, cbmgmtx.ErrScopeExists) {
			return nil, s.errorHandler.NewScopeExistsStatus(err, in.BucketName, in.ScopeName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	manifestUid, err := strconv.ParseUint(resp.ManifestUid, 16, 64)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	opts := gocbcorex.EnsureManifestOptions{
		BucketName:  in.BucketName,
		ManifestUid: manifestUid,
	}
	err = bucketAgent.EnsureManifest(ctx, &opts)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_collection_v1.CreateScopeResponse{}, nil
}

func (s *CollectionAdminServer) DeleteScope(
	ctx context.Context,
	in *admin_collection_v1.DeleteScopeRequest,
) (*admin_collection_v1.DeleteScopeResponse, error) {
	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	_, err := bucketAgent.DeleteScope(ctx, &cbmgmtx.DeleteScopeOptions{
		OnBehalfOf: oboInfo,
		BucketName: in.BucketName,
		ScopeName:  in.ScopeName,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		} else if errors.Is(err, cbmgmtx.ErrScopeNotFound) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_collection_v1.DeleteScopeResponse{}, nil
}

func (s *CollectionAdminServer) CreateCollection(
	ctx context.Context,
	in *admin_collection_v1.CreateCollectionRequest,
) (*admin_collection_v1.CreateCollectionResponse, error) {
	apiVersion, err := apiversion.FromContext(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}

	var noExpirySupported bool
	if apiVersion >= apiversion.CollectionNoExpiry {
		noExpirySupported = true
	}

	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var maxTTL int32
	if in.MaxExpirySecs != nil {
		if *in.MaxExpirySecs > 0 {
			maxTTL = int32(*in.MaxExpirySecs)
		} else if noExpirySupported {
			maxTTL = -1
		}
	}

	resp, err := bucketAgent.CreateCollection(ctx, &cbmgmtx.CreateCollectionOptions{
		OnBehalfOf:     oboInfo,
		BucketName:     in.BucketName,
		CollectionName: in.CollectionName,
		ScopeName:      in.ScopeName,
		MaxTTL:         maxTTL,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		} else if errors.Is(err, cbmgmtx.ErrCollectionExists) {
			return nil, s.errorHandler.NewCollectionExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, cbmgmtx.ErrScopeNotFound) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	manifestUid, err := strconv.ParseUint(resp.ManifestUid, 16, 64)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	opts := gocbcorex.EnsureManifestOptions{
		BucketName:  in.BucketName,
		ManifestUid: manifestUid,
	}
	err = bucketAgent.EnsureManifest(ctx, &opts)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_collection_v1.CreateCollectionResponse{}, nil
}

func (s *CollectionAdminServer) DeleteCollection(
	ctx context.Context,
	in *admin_collection_v1.DeleteCollectionRequest,
) (*admin_collection_v1.DeleteCollectionResponse, error) {
	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	resp, err := bucketAgent.DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
		OnBehalfOf:     oboInfo,
		BucketName:     in.BucketName,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		} else if errors.Is(err, cbmgmtx.ErrCollectionNotFound) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, cbmgmtx.ErrScopeNotFound) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	manifestUid, err := strconv.ParseUint(resp.ManifestUid, 16, 64)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	opts := gocbcorex.EnsureManifestOptions{
		BucketName:  in.BucketName,
		ManifestUid: manifestUid,
	}
	err = bucketAgent.EnsureManifest(ctx, &opts)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_collection_v1.DeleteCollectionResponse{}, nil
}

func (s *CollectionAdminServer) UpdateCollection(
	ctx context.Context,
	in *admin_collection_v1.UpdateCollectionRequest,
) (*admin_collection_v1.UpdateCollectionResponse, error) {
	apiVersion, err := apiversion.FromContext(ctx)
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}

	var noExpirySupported bool
	if apiVersion >= apiversion.CollectionNoExpiry {
		noExpirySupported = true
	}

	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var maxTTL *int32
	if in.MaxExpirySecs != nil {
		if *in.MaxExpirySecs == 0 && noExpirySupported {
			maxTTL = ptr.To(int32(-1))
		} else if *in.MaxExpirySecs != 0 {
			maxTTL = ptr.To(int32(*in.MaxExpirySecs))
		}
	}

	_, err = bucketAgent.UpdateCollection(ctx, &cbmgmtx.UpdateCollectionOptions{
		OnBehalfOf:     oboInfo,
		BucketName:     in.BucketName,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
		MaxTTL:         maxTTL,
		HistoryEnabled: in.HistoryRetentionEnabled,
	})

	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		} else if errors.Is(err, cbmgmtx.ErrCollectionNotFound) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, cbmgmtx.ErrScopeNotFound) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, cbmgmtx.ErrServerInvalidArg) {
			return nil, s.errorHandler.NewCollectionInvalidArgStatus(err, "", in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_collection_v1.UpdateCollectionResponse{}, nil
}
