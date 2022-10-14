package pssystem

import (
	"context"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/couchbase/stellar-nebula/genproto/admin_bucket_v1"
	"github.com/couchbase/stellar-nebula/genproto/analytics_v1"
	"github.com/couchbase/stellar-nebula/genproto/couchbase_v1"
	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/genproto/search_v1"
	"github.com/couchbase/stellar-nebula/genproto/transactions_v1"
	"github.com/couchbase/stellar-nebula/psimpl"
)

// TODO(brett19): Implement the gateway system as its own component

type SystemOptions struct {
	Logger *zap.Logger

	BindAddress string
	BindPort    int

	Impl psimpl.Impl
}

type System struct {
	logger      *zap.Logger
	bindAddress string
	bindPort    int
	impl        psimpl.Impl

	grpcListener net.Listener
	grpcServer   *grpc.Server
}

func NewSystem(opts *SystemOptions) (*System, error) {
	impl := opts.Impl

	srv := grpc.NewServer()
	couchbase_v1.RegisterCouchbaseServer(srv, impl.CouchbaseV1())
	routing_v1.RegisterRoutingServer(srv, impl.RoutingV1())
	data_v1.RegisterDataServer(srv, impl.DataV1())
	query_v1.RegisterQueryServer(srv, impl.QueryV1())
	search_v1.RegisterSearchServer(srv, impl.SearchV1())
	analytics_v1.RegisterAnalyticsServer(srv, impl.AnalyticsV1())
	admin_bucket_v1.RegisterBucketAdminServer(srv, impl.AdminBucketV1())
	transactions_v1.RegisterTransactionsServer(srv, impl.TransactionsV1())

	s := &System{
		logger:      opts.Logger,
		bindAddress: opts.BindAddress,
		bindPort:    opts.BindPort,
		impl:        opts.Impl,
		grpcServer:  srv,
	}

	return s, nil
}

func (s *System) Serve(ctx context.Context, l net.Listener) error {
	return s.grpcServer.Serve(l)
}
