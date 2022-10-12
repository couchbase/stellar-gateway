package pssystem

import (
	"context"
	"fmt"
	"log"
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
	s := &System{
		logger:      opts.Logger,
		bindAddress: opts.BindAddress,
		bindPort:    opts.BindPort,
		impl:        opts.Impl,
	}

	err := s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *System) init() error {
	srv := grpc.NewServer()
	couchbase_v1.RegisterCouchbaseServer(srv, s.impl.CouchbaseV1())
	routing_v1.RegisterRoutingServer(srv, s.impl.RoutingV1())
	data_v1.RegisterDataServer(srv, s.impl.DataV1())
	query_v1.RegisterQueryServer(srv, s.impl.QueryV1())
	search_v1.RegisterSearchServer(srv, s.impl.SearchV1())
	analytics_v1.RegisterAnalyticsServer(srv, s.impl.AnalyticsV1())
	admin_bucket_v1.RegisterBucketAdminServer(srv, s.impl.AdminBucketV1())
	transactions_v1.RegisterTransactionsServer(srv, s.impl.TransactionsV1())
	s.grpcServer = srv

	// TODO(brett19): Consider whether creating the listener here is a good idea...
	// Its possible that it might take a while to initialize everything before we can
	// start actually accepting clients from these listeners.  On the flip-side, we need
	// the listeners started early so we know what addr/port we are using...  Might need
	// to split this into 3 calls.  init(), Prepare(), Run() to get that behaviour.

	log.Printf("initializing grpc listener")
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.bindAddress, s.bindPort))
	if err != nil {
		return err
	}
	log.Printf("grpc listener is listening at %v", lis.Addr())
	s.grpcListener = lis

	return nil
}

func (s *System) Run(ctx context.Context) error {
	err := s.grpcServer.Serve(s.grpcListener)
	if err != nil {
		log.Printf("failed to serve grpc: %v", err)
		return err
	}

	return nil
}
