package system

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/couchbase/stellar-gateway/contrib/oapimetrics"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/couchbase/stellar-gateway/gateway/apiversion"
	"github.com/couchbase/stellar-gateway/gateway/dapiimpl"
	"github.com/couchbase/stellar-gateway/gateway/dataimpl"
	"github.com/couchbase/stellar-gateway/gateway/hooks"
	"github.com/couchbase/stellar-gateway/gateway/ratelimiting"
	"github.com/couchbase/stellar-gateway/gateway/sdimpl"
	"github.com/couchbase/stellar-gateway/pkg/interceptors"
	"github.com/couchbase/stellar-gateway/pkg/metrics"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/oapi-codegen/runtime/strictmiddleware/nethttp"
	"github.com/rs/cors"
)

const maxMsgSize = 25 * 1024 * 1024 // 25MiB

type SystemOptions struct {
	Logger *zap.Logger

	DataImpl *dataimpl.Servers
	SdImpl   *sdimpl.Servers
	DapiImpl *dapiimpl.Servers
	Metrics  *metrics.SnMetrics

	RateLimiter    ratelimiting.RateLimiter
	GrpcTlsConfig  *tls.Config
	DapiTlsConfig  *tls.Config
	AlphaEndpoints bool
	Debug          bool
}

type System struct {
	logger *zap.Logger

	dataServer *grpc.Server
	sdServer   *grpc.Server
	dapiServer *http.Server
}

func NewSystem(opts *SystemOptions) (*System, error) {
	dataImpl := opts.DataImpl
	sdImpl := opts.SdImpl
	dapiImpl := opts.DapiImpl

	hooksManager := hooks.NewHooksManager(opts.Logger.Named("hooks-manager"))
	debugInterceptor := interceptors.NewDebugInterceptor(opts.Logger.Named("grpc-debug"))
	metricsInterceptor := interceptors.NewMetricsInterceptor(opts.Metrics)

	recoveryHandler := func(p any) (err error) {
		opts.Logger.Error("a panic has been triggered", zap.Any("error: ", p))
		return status.Errorf(codes.Internal, "An internal error occurred.")
	}

	var unaryInterceptors []grpc.UnaryServerInterceptor
	unaryInterceptors = append(unaryInterceptors, metricsInterceptor.UnaryInterceptor())
	if opts.Debug {
		unaryInterceptors = append(unaryInterceptors, debugInterceptor.UnaryInterceptor())
	}
	unaryInterceptors = append(unaryInterceptors, hooksManager.UnaryInterceptor())
	if opts.RateLimiter != nil {
		unaryInterceptors = append(unaryInterceptors, opts.RateLimiter.GrpcUnaryInterceptor())
	}
	unaryInterceptors = append(unaryInterceptors, apiversion.GrpcUnaryInterceptor(opts.Logger))
	unaryInterceptors = append(unaryInterceptors, recovery.UnaryServerInterceptor(
		recovery.WithRecoveryHandler(recoveryHandler),
	))

	var streamInterceptors []grpc.StreamServerInterceptor
	streamInterceptors = append(streamInterceptors, metricsInterceptor.StreamInterceptor())
	if opts.Debug {
		streamInterceptors = append(streamInterceptors, debugInterceptor.StreamInterceptor())
	}
	if opts.RateLimiter != nil {
		streamInterceptors = append(streamInterceptors, opts.RateLimiter.GrpcStreamInterceptor())
	}
	streamInterceptors = append(streamInterceptors, apiversion.GrpcStreamInterceptor(opts.Logger))
	streamInterceptors = append(streamInterceptors, recovery.StreamServerInterceptor(
		recovery.WithRecoveryHandler(recoveryHandler),
	))

	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
		grpc.Creds(credentials.NewTLS(opts.GrpcTlsConfig)),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.NumStreamWorkers(uint32(runtime.NumCPU()) * 12),
	}

	switch otel.GetMeterProvider().(type) {
	case noop.MeterProvider:
	default:
		serverOpts = append(serverOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	dataSrv := grpc.NewServer(serverOpts...)

	internal_hooks_v1.RegisterHooksServiceServer(dataSrv, hooksManager.Server())
	kv_v1.RegisterKvServiceServer(dataSrv, dataImpl.KvV1Server)
	query_v1.RegisterQueryServiceServer(dataSrv, dataImpl.QueryV1Server)
	search_v1.RegisterSearchServiceServer(dataSrv, dataImpl.SearchV1Server)
	admin_bucket_v1.RegisterBucketAdminServiceServer(dataSrv, dataImpl.AdminBucketV1Server)
	admin_collection_v1.RegisterCollectionAdminServiceServer(dataSrv, dataImpl.AdminCollectionV1Server)
	admin_query_v1.RegisterQueryAdminServiceServer(dataSrv, dataImpl.AdminQueryIndexV1Server)
	admin_search_v1.RegisterSearchAdminServiceServer(dataSrv, dataImpl.AdminSearchIndexV1Server)

	// health check
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	services := dataSrv.GetServiceInfo()
	for serviceName := range services {
		healthServer.SetServingStatus(serviceName, grpc_health_v1.HealthCheckResponse_SERVING)
	}
	grpc_health_v1.RegisterHealthServer(dataSrv, healthServer)

	sdSrv := grpc.NewServer(serverOpts...)

	internal_hooks_v1.RegisterHooksServiceServer(sdSrv, hooksManager.Server())
	routing_v1.RegisterRoutingServiceServer(sdSrv, sdImpl.RoutingV1Server)

	// data api
	sh := dataapiv1.NewStrictHandlerWithOptions(dapiImpl.DataApiV1Server, []nethttp.StrictHTTPMiddlewareFunc{
		dapiimpl.NewErrorHandler(opts.Logger),
		dapiimpl.NewOtelTracingHandler(),
		dapiimpl.NewUserAgentMetricsHandler(),
		oapimetrics.NewStatsHandler(opts.Logger),
	}, dataapiv1.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			opts.Logger.Error("handling a data api request", zap.Any("error: ", err))
			http.Error(
				w,
				`{"code": "Internal", "message": "an internal error occured, please contact support"}`,
				http.StatusInternalServerError)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			opts.Logger.Error("handling a data api response", zap.Any("error: ", err))
			http.Error(
				w,
				`{"code": "Internal", "message": "an internal error occured, please contact support"}`,
				http.StatusInternalServerError)
		},
	})

	mux := http.NewServeMux()
	mux.Handle("/v1/", dataapiv1.Handler(sh))
	mux.Handle("/_p/", dapiImpl.DataApiProxy)
	if opts.AlphaEndpoints {
		mux.Handle("/v1.alpha/", dataapiv1.Handler(sh))
	}

	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		AllowCredentials: true,
		Debug:            opts.Debug,
	})

	var httpHandler http.Handler = mux
	if opts.RateLimiter != nil {
		httpHandler = opts.RateLimiter.HttpMiddleware(httpHandler)
	}
	httpHandler = c.Handler(httpHandler)

	dapiSrv := &http.Server{
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      httpHandler,
		TLSConfig:    opts.DapiTlsConfig,
	}

	s := &System{
		logger:     opts.Logger,
		dataServer: dataSrv,
		sdServer:   sdSrv,
		dapiServer: dapiSrv,
	}

	return s, nil
}

func (s *System) Serve(ctx context.Context, l *Listeners) error {
	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		s.dataServer.Stop()
		s.sdServer.Stop()
		s.dapiServer.Close()
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

	if l.dapiListener != nil {
		wg.Add(1)
		go func() {
			err := s.dapiServer.ServeTLS(l.dapiListener, "", "")
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Warn("data api server serve failed", zap.Error(err))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

func (s *System) Shutdown() {
	if s.dataServer != nil {
		s.dataServer.GracefulStop()
	}

	if s.sdServer != nil {
		s.sdServer.GracefulStop()
	}

	if s.sdServer != nil {
		_ = s.dapiServer.Shutdown(context.Background())
	}
}
