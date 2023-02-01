package server_v1

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearchServer struct {
	search_v1.UnimplementedSearchServer

	cbClient *gocb.Cluster
}

func (s *SearchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.Search_SearchQueryServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchQuery not implemented")
}

func NewSearchServer(cbClient *gocb.Cluster) *SearchServer {
	return &SearchServer{
		cbClient: cbClient,
	}
}
