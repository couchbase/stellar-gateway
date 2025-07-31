package server_v1

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/zap"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbsearchx"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
)

type SearchIndexAdminServer struct {
	admin_search_v1.UnimplementedSearchAdminServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewSearchIndexAdminServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *SearchIndexAdminServer {
	return &SearchIndexAdminServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *SearchIndexAdminServer) CreateIndex(ctx context.Context, in *admin_search_v1.CreateIndexRequest) (*admin_search_v1.CreateIndexResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	index := cbsearchx.Index{
		Name: in.Name,
		Type: in.Type,
	}

	index.Params = make(map[string]json.RawMessage, len(in.Params))
	for key, param := range in.Params {
		index.Params[key] = param
	}

	index.PlanParams = make(map[string]json.RawMessage, len(in.PlanParams))
	for key, param := range in.PlanParams {
		index.PlanParams[key] = param
	}

	if in.PrevIndexUuid != nil {
		index.PrevIndexUUID = in.GetPrevIndexUuid()
	}

	if in.SourceName != nil {
		index.SourceName = in.GetSourceName()
	}

	index.SourceParams = make(map[string]json.RawMessage, len(in.SourceParams))
	for key, param := range in.SourceParams {
		index.SourceParams[key] = param
	}

	if in.SourceType != nil {
		index.SourceType = in.GetSourceType()
	}

	if in.SourceUuid != nil {
		index.SourceUUID = in.GetSourceUuid()
	}

	opts := &cbsearchx.UpsertIndexOptions{
		OnBehalfOf: oboInfo,
		Index:      index,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	res, err := agent.UpsertSearchIndex(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexExists) {
			return nil, s.errorHandler.NewSearchIndexExistsStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	err = agent.EnsureSearchIndex(ctx, &gocbcorex.EnsureSearchIndexOptions{
		IndexName:  opts.Name,
		BucketName: opts.BucketName,
		ScopeName:  opts.ScopeName,
		IndexUUID:  res.UUID,
		OnBehalfOf: oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.CreateIndexResponse{}, nil
}

func (s *SearchIndexAdminServer) UpdateIndex(ctx context.Context, in *admin_search_v1.UpdateIndexRequest) (*admin_search_v1.UpdateIndexResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	if in.Index == nil {
		return nil, status.New(codes.InvalidArgument, "Update index request missing index.").Err()
	}

	if in.Index.Uuid == "" {
		return nil, status.New(codes.InvalidArgument, "Update index request missing uuid.").Err()
	}

	index := cbsearchx.Index{
		Name: in.Index.Name,
		Type: in.Index.Type,
		UUID: in.Index.Uuid,
	}

	if in.Index.Params != nil {
		index.Params = make(map[string]json.RawMessage, len(in.Index.Params))
		for key, param := range in.Index.Params {
			index.Params[key] = param
		}
	}

	if in.Index.PlanParams != nil {
		index.PlanParams = make(map[string]json.RawMessage, len(in.Index.PlanParams))
		for key, param := range in.Index.PlanParams {
			index.PlanParams[key] = param
		}
	}

	if in.Index.SourceName != nil {
		index.SourceName = in.Index.GetSourceName()
	}

	if in.Index.SourceParams != nil {
		index.SourceParams = make(map[string]json.RawMessage, len(in.Index.SourceParams))
		for key, param := range in.Index.SourceParams {
			index.SourceParams[key] = param
		}
	}

	if in.Index.SourceType != nil {
		index.SourceType = in.Index.GetSourceType()
	}

	if in.Index.SourceUuid != nil {
		index.SourceUUID = in.Index.GetSourceUuid()
	}

	opts := &cbsearchx.UpsertIndexOptions{
		OnBehalfOf: oboInfo,
		Index:      index,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	res, err := agent.UpsertSearchIndex(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Index.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Index.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexExists) {
			return nil, s.errorHandler.NewSearchUUIDMismatchStatus(err, in.Index.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	err = agent.EnsureSearchIndex(ctx, &gocbcorex.EnsureSearchIndexOptions{
		IndexName:  opts.Name,
		BucketName: opts.BucketName,
		ScopeName:  opts.ScopeName,
		IndexUUID:  res.UUID,
		OnBehalfOf: oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.UpdateIndexResponse{}, nil
}

func (s *SearchIndexAdminServer) DeleteIndex(ctx context.Context, in *admin_search_v1.DeleteIndexRequest) (*admin_search_v1.DeleteIndexResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.DeleteIndexOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.DeleteSearchIndex(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	err = agent.EnsureSearchIndex(ctx, &gocbcorex.EnsureSearchIndexOptions{
		IndexName:   opts.IndexName,
		BucketName:  opts.BucketName,
		ScopeName:   opts.ScopeName,
		WantMissing: true,
		OnBehalfOf:  oboInfo,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.DeleteIndexResponse{}, nil
}

func (s *SearchIndexAdminServer) GetIndex(ctx context.Context, in *admin_search_v1.GetIndexRequest) (*admin_search_v1.GetIndexResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.GetIndexOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	index, err := agent.GetSearchIndex(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	adminIndex := cbsearchxIndexToPS(index)

	return &admin_search_v1.GetIndexResponse{
		Index: adminIndex,
	}, nil
}

func (s *SearchIndexAdminServer) ListIndexes(ctx context.Context, in *admin_search_v1.ListIndexesRequest) (*admin_search_v1.ListIndexesResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.GetAllIndexesOptions{
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	indexes, err := agent.GetAllSearchIndexes(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, "").Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	adminIndexes := make([]*admin_search_v1.Index, len(indexes))
	for i, index := range indexes {
		adminIndex := cbsearchxIndexToPS(&index)
		adminIndexes[i] = adminIndex
	}

	return &admin_search_v1.ListIndexesResponse{
		Indexes: adminIndexes,
	}, nil
}

func (s *SearchIndexAdminServer) AnalyzeDocument(ctx context.Context, in *admin_search_v1.AnalyzeDocumentRequest) (*admin_search_v1.AnalyzeDocumentResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.AnalyzeDocumentOptions{
		IndexName:  in.Name,
		DocContent: in.Doc,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	analysis, err := agent.AnalyzeDocument(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.AnalyzeDocumentResponse{
		Status:   analysis.Status,
		Analyzed: analysis.Analyzed,
	}, nil
}

func (s *SearchIndexAdminServer) GetIndexedDocumentsCount(ctx context.Context, in *admin_search_v1.GetIndexedDocumentsCountRequest) (*admin_search_v1.GetIndexedDocumentsCountResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.GetIndexedDocumentsCountOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	count, err := agent.GetSearchIndexedDocumentsCount(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.GetIndexedDocumentsCountResponse{
		Count: count,
	}, nil
}

func (s *SearchIndexAdminServer) PauseIndexIngest(ctx context.Context, in *admin_search_v1.PauseIndexIngestRequest) (*admin_search_v1.PauseIndexIngestResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.PauseIngestOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.PauseSearchIndexIngest(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.PauseIndexIngestResponse{}, nil
}

func (s *SearchIndexAdminServer) ResumeIndexIngest(ctx context.Context, in *admin_search_v1.ResumeIndexIngestRequest) (*admin_search_v1.ResumeIndexIngestResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.ResumeIngestOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.ResumeSearchIndexIngest(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.ResumeIndexIngestResponse{}, nil
}

func (s *SearchIndexAdminServer) AllowIndexQuerying(ctx context.Context, in *admin_search_v1.AllowIndexQueryingRequest) (*admin_search_v1.AllowIndexQueryingResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.AllowQueryingOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.AllowSearchIndexQuerying(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.AllowIndexQueryingResponse{}, nil
}

func (s *SearchIndexAdminServer) DisallowIndexQuerying(ctx context.Context, in *admin_search_v1.DisallowIndexQueryingRequest) (*admin_search_v1.DisallowIndexQueryingResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.DisallowQueryingOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.DisallowSearchIndexQuerying(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.DisallowIndexQueryingResponse{}, nil
}

func (s *SearchIndexAdminServer) FreezeIndexPlan(ctx context.Context, in *admin_search_v1.FreezeIndexPlanRequest) (*admin_search_v1.FreezeIndexPlanResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.FreezePlanOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.FreezeSearchIndexPlan(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.FreezeIndexPlanResponse{}, nil
}

func (s *SearchIndexAdminServer) UnfreezeIndexPlan(ctx context.Context, in *admin_search_v1.UnfreezeIndexPlanRequest) (*admin_search_v1.UnfreezeIndexPlanResponse, error) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	opts := &cbsearchx.UnfreezePlanOptions{
		IndexName:  in.Name,
		OnBehalfOf: oboInfo,
	}

	if in.BucketName != nil {
		opts.BucketName = in.GetBucketName()
	}

	if in.ScopeName != nil {
		opts.ScopeName = in.GetScopeName()
	}

	err := agent.UnfreezeSearchIndexPlan(ctx, opts)
	if err != nil {
		if errors.Is(err, gocbcorex.ErrServiceNotAvailable) {
			return nil, s.errorHandler.NewSearchServiceNotAvailableStatus(err, in.Name).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return nil, s.errorHandler.NewSearchIndexMissingStatus(err, in.Name).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &admin_search_v1.UnfreezeIndexPlanResponse{}, nil
}

func cbsearchxIndexToPS(index *cbsearchx.Index) *admin_search_v1.Index {
	adminIndex := &admin_search_v1.Index{
		Name: index.Name,
		Type: index.Type,
		Uuid: index.UUID,
	}

	if index.SourceName != "" {
		adminIndex.SourceName = &index.SourceName
	}

	if index.SourceType != "" {
		adminIndex.SourceType = &index.SourceType
	}

	if index.SourceUUID != "" {
		adminIndex.SourceUuid = &index.SourceUUID
	}

	if len(index.SourceParams) > 0 {
		adminIndex.SourceParams = make(map[string][]byte, len(index.SourceParams))
		for name, param := range index.SourceParams {
			adminIndex.SourceParams[name] = param
		}
	}

	if len(index.Params) > 0 {
		adminIndex.Params = make(map[string][]byte, len(index.Params))
		for name, param := range index.Params {
			adminIndex.Params[name] = param
		}
	}

	if len(index.PlanParams) > 0 {
		adminIndex.PlanParams = make(map[string][]byte, len(index.PlanParams))
		for name, param := range index.PlanParams {
			adminIndex.PlanParams[name] = param
		}
	}

	return adminIndex
}
