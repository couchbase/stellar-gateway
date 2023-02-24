package server_v1

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearchServer struct {
	search_v1.UnimplementedSearchServer
	log      *zap.Logger
	cbClient *gocb.Cluster
}

func (s *SearchServer) UpdateClient(client *gocb.Cluster) {
	s.cbClient = client
}

func (s *SearchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.Search_SearchQueryServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchQuery not implemented")
}

func NewSearchServer(cbClient *gocb.Cluster, logger *zap.Logger) *SearchServer {
	return &SearchServer{
		cbClient: cbClient,
		log:      logger,
	}
}
