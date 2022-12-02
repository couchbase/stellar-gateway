package server_v1

import (
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/genproto/admin_bucket_v1"
)

type BucketAdminServer struct {
	admin_bucket_v1.UnimplementedBucketAdminServer

	cbClient *gocb.Cluster
}

func NewBucketAdminServer(cbClient *gocb.Cluster) *BucketAdminServer {
	return &BucketAdminServer{
		cbClient: cbClient,
	}
}
