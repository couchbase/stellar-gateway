package server_v1

import (
	"context"
	"errors"
	"strconv"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"go.uber.org/zap"
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
				collectionSpec.MaxExpirySecs = &collection.MaxTTL
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

	_, err := bucketAgent.CreateScope(ctx, &cbmgmtx.CreateScopeOptions{
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
	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var maxTTL uint32
	if in.MaxExpirySecs != nil {
		maxTTL = *in.MaxExpirySecs
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
	bucketAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	_, err := bucketAgent.UpdateCollection(ctx, &cbmgmtx.UpdateCollectionOptions{
		OnBehalfOf:     oboInfo,
		BucketName:     in.BucketName,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
		MaxTTL:         in.MaxExpirySecs,
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
