package server_v1

import (
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearchServer struct {
	search_v1.UnimplementedSearchServiceServer

	logger      *zap.Logger
	authHandler *AuthHandler
}

func NewSearchServer(
	logger *zap.Logger,
	authHandler *AuthHandler,
) *SearchServer {
	return &SearchServer{
		logger:      logger,
		authHandler: authHandler,
	}
}

func (s *SearchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.SearchService_SearchQueryServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchQuery not implemented")
}
