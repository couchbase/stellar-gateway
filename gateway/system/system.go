package system

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/couchbase/goprotostellar/genproto/transactions_v1"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/hooks"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/pkg/interceptors"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
)

// TODO(brett19): Implement the gateway system as its own component

type SystemOptions struct {
	Logger *zap.Logger

	DataImpl *dataimpl.Servers
	SdImpl   *sdimpl.Servers
	Metrics  *metrics.SnMetrics

	// TODO(abose): tidy up maybe move to somewhere more appropriate.
	DataImplServerOpts []grpc.ServerOption
}

type System struct {
	logger *zap.Logger

	dataServer *grpc.Server
	sdServer   *grpc.Server
}

func NewSystem(opts *SystemOptions) (*System, error) {
	dataImpl := opts.DataImpl
	sdImpl := opts.SdImpl

	hooksManager := hooks.NewHooksManager(opts.Logger.Named("hooks-manager"))
	metricsInterceptor := interceptors.NewMetricsInterceptor(opts.Metrics)

	dataImpSrvOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(hooksManager.UnaryInterceptor(), metricsInterceptor.UnaryConnectionCounterInterceptor),
	}
	if len(opts.DataImplServerOpts) > 0 {
		dataImpSrvOpts = append(dataImpSrvOpts, opts.DataImplServerOpts...)
	}

	dataSrv := grpc.NewServer(dataImpSrvOpts...)

	internal_hooks_v1.RegisterHooksServiceServer(dataSrv, hooksManager.Server())
	kv_v1.RegisterKvServiceServer(dataSrv, dataImpl.KvV1Server)
	query_v1.RegisterQueryServiceServer(dataSrv, dataImpl.QueryV1Server)
	search_v1.RegisterSearchServiceServer(dataSrv, dataImpl.SearchV1Server)
	analytics_v1.RegisterAnalyticsServiceServer(dataSrv, dataImpl.AnalyticsV1Server)
	admin_bucket_v1.RegisterBucketAdminServiceServer(dataSrv, dataImpl.AdminBucketV1Server)
	admin_collection_v1.RegisterCollectionAdminServiceServer(dataSrv, dataImpl.AdminCollectionV1Server)
	transactions_v1.RegisterTransactionsServiceServer(dataSrv, dataImpl.TransactionsV1Server)

	// health check
	grpc_health_v1.RegisterHealthServer(dataSrv, HealthV1Server{})

	sdSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(hooksManager.UnaryInterceptor(), metricsInterceptor.UnaryConnectionCounterInterceptor),
	)
	internal_hooks_v1.RegisterHooksServiceServer(sdSrv, hooksManager.Server())
	routing_v1.RegisterRoutingServiceServer(sdSrv, sdImpl.RoutingV1Server)

	s := &System{
		logger:     opts.Logger,
		dataServer: dataSrv,
		sdServer:   sdSrv,
	}

	return s, nil
}

func (s *System) Serve(ctx context.Context, l *Listeners) error {
	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		s.dataServer.Stop()
		s.sdServer.Stop()
	}()

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
