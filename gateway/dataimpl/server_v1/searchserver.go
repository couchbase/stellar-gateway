package server_v1

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearchServer struct {
	search_v1.UnimplementedSearchServiceServer
	logger   *zap.Logger
	cbClient *gocbcorex.AgentManager
}

func (s *SearchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.SearchService_SearchQueryServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchQuery not implemented")
}

func NewSearchServer(cbClient *gocbcorex.AgentManager, logger *zap.Logger) *SearchServer {
	return &SearchServer{
		cbClient: cbClient,
		logger:   logger,
	}
}
