package system

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/couchbase/stellar-nebula/gateway/dataimpl"
	"github.com/couchbase/stellar-nebula/gateway/sdimpl"
	"github.com/couchbase/stellar-nebula/genproto/admin_bucket_v1"
	"github.com/couchbase/stellar-nebula/genproto/analytics_v1"
	"github.com/couchbase/stellar-nebula/genproto/couchbase_v1"
	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/genproto/search_v1"
	"github.com/couchbase/stellar-nebula/genproto/transactions_v1"
)

// TODO(brett19): Implement the gateway system as its own component

type SystemOptions struct {
	Logger *zap.Logger

	DataImpl *dataimpl.Servers
	SdImpl   *sdimpl.Servers
}

type System struct {
	logger *zap.Logger

	dataServer *grpc.Server
	sdServer   *grpc.Server
}

func NewSystem(opts *SystemOptions) (*System, error) {
	dataImpl := opts.DataImpl
	sdImpl := opts.SdImpl

	dataSrv := grpc.NewServer()
	couchbase_v1.RegisterCouchbaseServer(dataSrv, dataImpl.CouchbaseV1Server)
	data_v1.RegisterDataServer(dataSrv, dataImpl.DataV1Server)
	query_v1.RegisterQueryServer(dataSrv, dataImpl.QueryV1Server)
	search_v1.RegisterSearchServer(dataSrv, dataImpl.SearchV1Server)
	analytics_v1.RegisterAnalyticsServer(dataSrv, dataImpl.AnalyticsV1Server)
	admin_bucket_v1.RegisterBucketAdminServer(dataSrv, dataImpl.AdminBucketV1Server)
	transactions_v1.RegisterTransactionsServer(dataSrv, dataImpl.TransactionsV1Server)

	sdSrv := grpc.NewServer()
	routing_v1.RegisterRoutingServer(sdSrv, sdImpl.RoutingV1Server)

	s := &System{
		logger:     opts.Logger,
		dataServer: dataSrv,
		sdServer:   sdSrv,
	}

	return s, nil
}

func (s *System) Serve(ctx context.Context, l *Listeners) error {
	var wg sync.WaitGroup

	if l.dataListener != nil {
		wg.Add(1)
		go func() {
			err := s.dataServer.Serve(l.dataListener)
			if err != nil {
				s.logger.Warn("data server serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	if l.sdListener != nil {
		wg.Add(1)
		go func() {
			err := s.sdServer.Serve(l.sdListener)
			if err != nil {
				s.logger.Warn("service discovery server serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}
