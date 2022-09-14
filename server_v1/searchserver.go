package server_v1

import (
	"github.com/couchbase/gocb/v2"
	search_v1 "github.com/couchbase/stellar-nebula/genproto/search/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type searchServer struct {
	search_v1.UnimplementedSearchServer

	cbClient *gocb.Cluster
}

func (s *searchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.Search_SearchQueryServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchQuery not implemented")
}

func NewSearchServer(cbClient *gocb.Cluster) *searchServer {
	return &searchServer{
		cbClient: cbClient,
	}
}
