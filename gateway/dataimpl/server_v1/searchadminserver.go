package server_v1

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex/cbsearchx"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
)

type SearchIndexAdminServer struct {
	admin_search_v1.UnimplementedSearchAdminServiceServer

	logger      *zap.Logger
	authHandler *AuthHandler
}

func NewSearchIndexAdminServer(
	logger *zap.Logger,
	authHandler *AuthHandler,
) *SearchIndexAdminServer {
	return &SearchIndexAdminServer{
		logger:      logger,
		authHandler: authHandler,
	}
}

func (s *SearchIndexAdminServer) UpsertIndex(ctx context.Context, in *admin_search_v1.UpsertIndexRequest) (*admin_search_v1.UpsertIndexResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	index := cbsearchx.Index{
		Name: in.Name,
		Type: in.Type,
	}

	for key, param := range in.Params {
		index.Params[key] = param
	}

	for key, param := range in.PlanParams {
		index.PlanParams[key] = param
	}

	if in.PrevIndexUuid != nil {
		index.PrevIndexUUID = in.GetPrevIndexUuid()
	}

	if in.SourceName != nil {
		index.SourceName = in.GetSourceName()
	}

	for key, param := range in.SourceParams {
		index.SourceParams[key] = param
	}

	if in.SourceType != nil {
		index.SourceType = in.GetSourceType()
	}

	if in.SourceUuid != nil {
		index.SourceUUID = in.GetSourceUuid()
	}

	if in.Uuid != nil {
		index.UUID = in.GetUuid()
	}

	err := agent.UpsertSearchIndex(ctx, &cbsearchx.UpsertIndexOptions{
		OnBehalfOf: oboInfo,
		Index:      index,
	})
	if err != nil {
		if errors.Is(err, cbsearchx.ErrIndexExists) {
			return nil, newSearchIndexExistsStatus(err, in.Name).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	return &admin_search_v1.UpsertIndexResponse{}, nil
}

func (s *SearchIndexAdminServer) DeleteIndex(ctx context.Context, in *admin_search_v1.DeleteIndexRequest) (*admin_search_v1.DeleteIndexResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := agent.DeleteSearchIndex(ctx, &cbsearchx.DeleteIndexOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	})
	if err != nil {
		if errors.Is(err, cbsearchx.ErrIndexExists) {
			return nil, newSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	return &admin_search_v1.DeleteIndexResponse{}, nil
}
