package server_v1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"go.uber.org/zap"
)

type QueryIndexAdminServer struct {
	admin_query_v1.UnimplementedQueryAdminServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewQueryIndexAdminServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *QueryIndexAdminServer {
	return &QueryIndexAdminServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *QueryIndexAdminServer) normalizeDefaultName(name *string) string {
	resourceName := "_default"
	if name != nil {
		resourceName = *name
	}

	return resourceName
}

func (s *QueryIndexAdminServer) validateNames(index, bucket, scope, col *string) error {
	var missingType string
	switch {
	case bucket != nil && *bucket == "":
		missingType = "Bucket"
	case scope != nil && *scope == "":
		missingType = "Scope"
	case col != nil && *col == "":
		missingType = "Collection"
	}

	if index != nil {
		if strings.ContainsAny(*index, `!,.$%^&*()+={}[]':;|\<>?@`) {
			return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
				nil,
				*index,
				"Index name cannot contain special characters").Err()
		}

		if len(*index) >= 220 {
			return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
				nil,
				*index,
				"Index name cannot be longer than 219 characters.").Err()
		}

		if *index == "" {
			return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
				nil,
				*index,
				"Index name cannot be an empty string.").Err()
		}

		if strings.Contains(*index, " ") {
			return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
				nil,
				*index,
				"Index name cannot contain spaces.").Err()
		}
	}

	var name string
	if index == nil {
		name = "#primary"
	} else {
		name = *index
	}

	scopeName := s.normalizeDefaultName(scope)
	colName := s.normalizeDefaultName(col)

	if bucket != nil && strings.Contains(*bucket, " ") {
		return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
			nil,
			name,
			fmt.Sprintf(`Bucket name '%s' cannot contain blank spaces.`, *bucket)).Err()
	}

	if strings.Contains(scopeName, " ") {
		return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
			nil,
			name,
			fmt.Sprintf(`Scope name '%s' cannot contain blank spaces.`, scopeName)).Err()
	}

	if strings.Contains(colName, " ") {
		return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
			nil,
			name,
			fmt.Sprintf(`Collection name '%s' cannot contain blank spaces.`, colName)).Err()
	}

	if missingType != "" {
		return s.errorHandler.NewQueryIndexInvalidArgumentStatus(
			nil,
			name,
			fmt.Sprintf("%s name cannot be an empty string.", missingType)).Err()
	}

	return nil
}

func (s *QueryIndexAdminServer) GetAllIndexes(
	ctx context.Context,
	in *admin_query_v1.GetAllIndexesRequest,
) (*admin_query_v1.GetAllIndexesResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	indexes, err := agent.GetAllIndexes(ctx, &cbqueryx.GetAllIndexesOptions{
		BucketName:     in.GetBucketName(),
		ScopeName:      in.GetScopeName(),
		CollectionName: in.GetCollectionName(),
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		if errors.Is(err, cbqueryx.ErrAuthenticationFailure) {
			return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
				err,
				in.GetBucketName(),
				in.GetScopeName(),
				in.GetCollectionName(),
			).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var protoIndexes []*admin_query_v1.GetAllIndexesResponse_Index
	for _, index := range indexes {
		state, errSt := indexStateFromQueryTableIndexState(index.State)
		if errSt != nil {
			return nil, errSt.Err()
		}

		protoIndex := &admin_query_v1.GetAllIndexesResponse_Index{
			Name:      index.Name,
			IsPrimary: index.IsPrimary,
			Type:      admin_query_v1.IndexType_INDEX_TYPE_GSI,
			State:     state,
			Fields:    index.IndexKey,
		}

		if index.BucketId == "" {
			defaultScopeColl := "_default"
			protoIndex.BucketName = index.KeyspaceId
			protoIndex.ScopeName = defaultScopeColl
			protoIndex.CollectionName = defaultScopeColl
		} else {
			protoIndex.BucketName = index.BucketId
			protoIndex.ScopeName = index.ScopeId
			protoIndex.CollectionName = index.KeyspaceId
		}

		if index.Condition != "" {
			protoIndex.Condition = &index.Condition
		}

		if index.Partition != "" {
			protoIndex.Partition = &index.Partition
		}

		protoIndexes = append(protoIndexes, protoIndex)
	}

	return &admin_query_v1.GetAllIndexesResponse{
		Indexes: protoIndexes,
	}, nil
}

func (s *QueryIndexAdminServer) CreatePrimaryIndex(
	ctx context.Context,
	in *admin_query_v1.CreatePrimaryIndexRequest,
) (*admin_query_v1.CreatePrimaryIndexResponse, error) {
	if err := s.validateNames(in.Name, &in.BucketName, in.ScopeName, in.CollectionName); err != nil {
		return nil, err
	}

	if in.NumReplicas != nil && *in.NumReplicas < 0 {
		msg := "number of index replicas cannot be negative"
		var name string
		if in.Name != nil {
			name = *in.Name
		}
		return nil, s.errorHandler.NewQueryIndexInvalidArgumentStatus(
			nil,
			name,
			msg).Err()
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var indexName string
	if in.Name == nil {
		indexName = "#primary"
	} else {
		indexName = *in.Name
	}

	err := agent.CreatePrimaryIndex(ctx, &cbqueryx.CreatePrimaryIndexOptions{
		BucketName:     in.BucketName,
		ScopeName:      in.GetScopeName(),
		CollectionName: in.GetCollectionName(),
		IndexName:      indexName,
		NumReplicas:    in.NumReplicas,
		Deferred:       in.Deferred,
		IgnoreIfExists: in.GetIgnoreIfExists(),
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		var rErr *cbqueryx.ResourceError
		if errors.As(err, &rErr) {
			if errors.Is(rErr.Cause, cbqueryx.ErrIndexExists) {
				return nil, s.errorHandler.NewQueryIndexExistsStatus(
					err,
					rErr.IndexName,
					in.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrAuthenticationFailure) {
				return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrScopeNotFound) {
				return nil, s.errorHandler.NewScopeMissingStatus(err, rErr.BucketName, rErr.ScopeName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrCollectionNotFound) {
				return nil, s.errorHandler.NewCollectionMissingStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}
		}

		var sErr cbqueryx.ServerInvalidArgError
		if errors.As(err, &sErr) {
			msg := fmt.Sprintf("invalid argument: %s - %s", sErr.Argument, sErr.Reason)
			return nil, s.errorHandler.NewQueryIndexInvalidArgumentStatus(
				err,
				indexName,
				msg).Err()
		}

		if errors.Is(err, cbqueryx.ErrAuthenticationFailure) {
			return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
				err,
				in.BucketName,
				in.GetScopeName(),
				in.GetCollectionName(),
			).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	scopeName := s.normalizeDefaultName(in.ScopeName)
	collectionName := s.normalizeDefaultName(in.CollectionName)

	err = agent.EnsureQueryIndexCreated(ctx, &gocbcorex.EnsureQueryIndexCreatedOptions{
		BucketName:     in.BucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		IndexName:      in.GetName(),
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_query_v1.CreatePrimaryIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) CreateIndex(
	ctx context.Context,
	in *admin_query_v1.CreateIndexRequest,
) (*admin_query_v1.CreateIndexResponse, error) {
	if err := s.validateNames(&in.Name, &in.BucketName, in.ScopeName, in.CollectionName); err != nil {
		return nil, err
	}

	if in.NumReplicas != nil && *in.NumReplicas < 0 {
		msg := "number of index replicas cannot be negative"
		return nil, s.errorHandler.NewQueryIndexInvalidArgumentStatus(
			nil,
			in.Name,
			msg).Err()
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := agent.CreateIndex(ctx, &cbqueryx.CreateIndexOptions{
		BucketName:     in.BucketName,
		ScopeName:      in.GetScopeName(),
		CollectionName: in.GetCollectionName(),
		IndexName:      in.Name,
		NumReplicas:    in.NumReplicas,
		Fields:         in.Fields,
		Deferred:       in.Deferred,
		IgnoreIfExists: in.GetIgnoreIfExists(),
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		var rErr *cbqueryx.ResourceError
		if errors.As(err, &rErr) {
			if errors.Is(rErr.Cause, cbqueryx.ErrIndexExists) {
				return nil, s.errorHandler.NewQueryIndexExistsStatus(
					err,
					rErr.IndexName,
					in.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrAuthenticationFailure) {
				return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrScopeNotFound) {
				return nil, s.errorHandler.NewScopeMissingStatus(err, rErr.BucketName, rErr.ScopeName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrCollectionNotFound) {
				return nil, s.errorHandler.NewCollectionMissingStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}
		}

		var sErr cbqueryx.ServerInvalidArgError
		if errors.As(err, &sErr) {
			msg := fmt.Sprintf("invalid argument: %s - %s", sErr.Argument, sErr.Reason)
			return nil, s.errorHandler.NewQueryIndexInvalidArgumentStatus(
				err,
				in.Name,
				msg).Err()
		}

		if errors.Is(err, cbqueryx.ErrAuthenticationFailure) {
			return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
				err,
				in.BucketName,
				in.GetScopeName(),
				in.GetCollectionName(),
			).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	scopeName := s.normalizeDefaultName(in.ScopeName)
	collectionName := s.normalizeDefaultName(in.CollectionName)

	err = agent.EnsureQueryIndexCreated(ctx, &gocbcorex.EnsureQueryIndexCreatedOptions{
		BucketName:     in.BucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		IndexName:      in.Name,
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_query_v1.CreateIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) DropPrimaryIndex(
	ctx context.Context,
	in *admin_query_v1.DropPrimaryIndexRequest,
) (*admin_query_v1.DropPrimaryIndexResponse, error) {
	if err := s.validateNames(in.Name, &in.BucketName, in.ScopeName, in.CollectionName); err != nil {
		return nil, err
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := agent.DropPrimaryIndex(ctx, &cbqueryx.DropPrimaryIndexOptions{
		BucketName:        in.BucketName,
		ScopeName:         in.GetScopeName(),
		CollectionName:    in.GetCollectionName(),
		IndexName:         in.GetName(),
		IgnoreIfNotExists: in.GetIgnoreIfMissing(),
		OnBehalfOf:        oboInfo,
	})
	if err != nil {
		var rErr *cbqueryx.ResourceError
		if errors.As(err, &rErr) {
			if errors.Is(rErr.Cause, cbqueryx.ErrIndexNotFound) {
				return nil, s.errorHandler.NewQueryIndexMissingStatus(
					err,
					rErr.IndexName,
					in.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrAuthenticationFailure) {
				return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrScopeNotFound) {
				return nil, s.errorHandler.NewScopeMissingStatus(err, rErr.BucketName, rErr.ScopeName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrCollectionNotFound) {
				return nil, s.errorHandler.NewCollectionMissingStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}
		}

		if errors.Is(err, cbqueryx.ErrAuthenticationFailure) {
			return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
				err,
				in.GetBucketName(),
				in.GetScopeName(),
				in.GetCollectionName()).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	scopeName := s.normalizeDefaultName(in.ScopeName)
	collectionName := s.normalizeDefaultName(in.CollectionName)

	err = agent.EnsureQueryIndexDropped(ctx, &gocbcorex.EnsureQueryIndexDroppedOptions{
		BucketName:     in.BucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		IndexName:      in.GetName(),
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_query_v1.DropPrimaryIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) DropIndex(
	ctx context.Context,
	in *admin_query_v1.DropIndexRequest,
) (*admin_query_v1.DropIndexResponse, error) {
	if err := s.validateNames(&in.Name, &in.BucketName, in.ScopeName, in.CollectionName); err != nil {
		return nil, err
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := agent.DropIndex(ctx, &cbqueryx.DropIndexOptions{
		BucketName:        in.BucketName,
		ScopeName:         in.GetScopeName(),
		CollectionName:    in.GetCollectionName(),
		IndexName:         in.Name,
		IgnoreIfNotExists: in.GetIgnoreIfMissing(),
		OnBehalfOf:        oboInfo,
	})
	if err != nil {
		var rErr *cbqueryx.ResourceError
		if errors.As(err, &rErr) {
			if errors.Is(rErr.Cause, cbqueryx.ErrIndexNotFound) {
				return nil, s.errorHandler.NewQueryIndexMissingStatus(
					err,
					rErr.IndexName,
					in.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrAuthenticationFailure) {
				return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrScopeNotFound) {
				return nil, s.errorHandler.NewScopeMissingStatus(err, rErr.BucketName, rErr.ScopeName).Err()
			}

			if errors.Is(rErr.Cause, cbqueryx.ErrCollectionNotFound) {
				return nil, s.errorHandler.NewCollectionMissingStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}
		}

		if errors.Is(err, cbqueryx.ErrAuthenticationFailure) {
			return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
				err,
				in.GetBucketName(),
				in.GetScopeName(),
				in.GetCollectionName()).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	scopeName := s.normalizeDefaultName(in.ScopeName)
	collectionName := s.normalizeDefaultName(in.CollectionName)

	err = agent.EnsureQueryIndexDropped(ctx, &gocbcorex.EnsureQueryIndexDroppedOptions{
		BucketName:     in.BucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		IndexName:      in.Name,
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_query_v1.DropIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) BuildDeferredIndexes(
	ctx context.Context,
	in *admin_query_v1.BuildDeferredIndexesRequest,
) (*admin_query_v1.BuildDeferredIndexesResponse, error) {
	err := s.validateNames(nil, &in.BucketName, in.ScopeName, in.CollectionName)
	if err != nil {
		return nil, err
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	deferredIndexes, err := agent.BuildDeferredIndexes(ctx, &cbqueryx.BuildDeferredIndexesOptions{
		BucketName:     in.BucketName,
		ScopeName:      in.GetScopeName(),
		CollectionName: in.GetCollectionName(),
		OnBehalfOf:     oboInfo,
	})
	if err != nil {
		var rErr *cbqueryx.ResourceError
		if errors.As(err, &rErr) {
			if errors.Is(rErr.Cause, cbqueryx.ErrAuthenticationFailure) {
				return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
					err,
					rErr.BucketName,
					rErr.ScopeName,
					rErr.CollectionName).Err()
			}
		}

		if errors.Is(err, cbqueryx.ErrAuthenticationFailure) {
			return nil, s.errorHandler.NewQueryIndexAuthenticationFailureStatus(
				err,
				in.BucketName,
				in.GetScopeName(),
				in.GetCollectionName(),
			).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var protoIndexes []*admin_query_v1.BuildDeferredIndexesResponse_Index
	for _, index := range deferredIndexes {
		var scopeName *string
		if index.ScopeName != "" {
			scopeName = &index.ScopeName
		}
		var collectionName *string
		if index.CollectionName != "" {
			collectionName = &index.CollectionName
		}
		protoIndex := &admin_query_v1.BuildDeferredIndexesResponse_Index{
			BucketName:     index.BucketName,
			ScopeName:      scopeName,
			CollectionName: collectionName,
			Name:           index.IndexName,
		}
		protoIndexes = append(protoIndexes, protoIndex)
	}

	return &admin_query_v1.BuildDeferredIndexesResponse{
		Indexes: protoIndexes,
	}, nil
}

func (s *QueryIndexAdminServer) WaitForIndexOnline(
	ctx context.Context,
	in *admin_query_v1.WaitForIndexOnlineRequest,
) (*admin_query_v1.WaitForIndexOnlineResponse, error) {
	scopeName := "_default"
	collectionName := "_default"
	if in.ScopeName != "" {
		scopeName = in.ScopeName
	}
	if in.CollectionName != "" {
		collectionName = in.CollectionName
	}

	for {
		watchIndexesResp, err := s.GetAllIndexes(ctx, &admin_query_v1.GetAllIndexesRequest{
			BucketName:     &in.BucketName,
			ScopeName:      &scopeName,
			CollectionName: &collectionName,
		})
		if err != nil {
			return nil, err
		}

		var foundIndex *admin_query_v1.GetAllIndexesResponse_Index
		for _, index := range watchIndexesResp.Indexes {
			if index.Name == in.Name &&
				index.CollectionName == collectionName &&
				index.ScopeName == scopeName &&
				index.BucketName == in.BucketName {
				foundIndex = index
			}
		}

		if foundIndex == nil {
			return nil, s.errorHandler.NewQueryIndexMissingStatus(
				nil,
				in.Name,
				in.BucketName,
				scopeName,
				collectionName).Err()
		}

		if foundIndex.State == admin_query_v1.IndexState_INDEX_STATE_DEFERRED {
			return nil, s.errorHandler.NewQueryIndexNotBuildingStatus(
				nil, in.BucketName, scopeName, collectionName, in.Name).Err()
		}

		if foundIndex.State == admin_query_v1.IndexState_INDEX_STATE_ONLINE {
			break
		}

		s.logger.Debug("waiting for an index which is not online",
			zap.String("currentState", foundIndex.State.String()))

		// if some of the indexes still haven't transitioned out of the deferred state,
		// we wait 100ms and then scan to see if the index has transitioned.
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		continue
	}

	return &admin_query_v1.WaitForIndexOnlineResponse{}, nil
}
