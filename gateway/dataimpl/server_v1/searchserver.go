package server_v1

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearchServer struct {
	search_v1.UnimplementedSearchServer

	cbClient *gocbcorex.AgentManager
}

func (s *SearchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.Search_SearchQueryServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchQuery not implemented")
}

func NewSearchServer(cbClient *gocbcorex.AgentManager) *SearchServer {
	return &SearchServer{
		cbClient: cbClient,
	}
}
