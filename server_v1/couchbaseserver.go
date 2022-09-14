package server_v1

import (
	"context"

	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
)

type couchbaseServer struct {
	couchbase_v1.UnimplementedCouchbaseServer
}

func (s *couchbaseServer) Hello(ctx context.Context, in *couchbase_v1.HelloRequest) (*couchbase_v1.HelloResponse, error) {
	return &couchbase_v1.HelloResponse{}, nil
}

func NewCouchbaseServer() *couchbaseServer {
	return &couchbaseServer{}
}
