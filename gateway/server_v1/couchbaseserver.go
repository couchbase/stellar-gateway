package server_v1

import (
	"context"

	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
)

type CouchbaseServer struct {
	couchbase_v1.UnimplementedCouchbaseServer
}

func (s *CouchbaseServer) Hello(ctx context.Context, in *couchbase_v1.HelloRequest) (*couchbase_v1.HelloResponse, error) {
	return &couchbase_v1.HelloResponse{}, nil
}

func NewCouchbaseServer() *CouchbaseServer {
	return &CouchbaseServer{}
}
