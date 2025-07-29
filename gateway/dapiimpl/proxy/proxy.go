package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var (
	buildVersion = buildversion.GetVersion("github.com/couchbase/stellar-gateway")
	meter        = otel.Meter("github.com/couchbase/stellar-gateway/gateway/dapiimpl/proxy",
		metric.WithInstrumentationVersion(buildVersion))
	tracer = otel.GetTracerProvider().Tracer("github.com/couchbase/stellar-gateway/gateway/dapiimpl/proxy")
)

type ServiceType string

const (
	ServiceTypeMgmt      ServiceType = "mgmt"
	ServiceTypeQuery     ServiceType = "query"
	ServiceTypeSearch    ServiceType = "search"
	ServiceTypeAnalytics ServiceType = "analytics"
	ServiceTypeEventing  ServiceType = "eventing"
)

type DataApiProxy struct {
	logger       *zap.Logger
	cbClient     *gocbcorex.BucketsTrackingAgentManager
	disableAdmin bool
	debugMode    bool
	mux          *http.ServeMux

	numFailures    metric.Int64Counter
	numRequests    metric.Int64Counter
	ttfbMillis     metric.Int64Histogram
	durationMillis metric.Int64Histogram
}

func NewDataApiProxy(
	logger *zap.Logger,
	cbClient *gocbcorex.BucketsTrackingAgentManager,
	services []ServiceType,
	disableAdmin bool,
	debugMode bool,
) *DataApiProxy {
	mux := http.NewServeMux()

	numFailures, err := meter.Int64Counter("dapi_proxy_failures")
	if err != nil {
		logger.Warn("failed to initialize failures counter", zap.Error(err))
	}

	numRequests, err := meter.Int64Counter("dapi_proxy_requests")
	if err != nil {
		logger.Warn("failed to initialize request counter", zap.Error(err))
	}

	ttfbMillis, err := meter.Int64Histogram("dapi_proxy_ttfb_milliseconds")
	if err != nil {
		logger.Warn("failed to initialize ttfb histogram", zap.Error(err))
	}

	durationMillis, err := meter.Int64Histogram("dapi_proxy_duration_milliseconds")
	if err != nil {
		logger.Warn("failed to initialize duration histogram", zap.Error(err))
	}

	proxy := &DataApiProxy{
		logger:         logger,
		cbClient:       cbClient,
		disableAdmin:   disableAdmin,
		debugMode:      debugMode,
		mux:            mux,
		numFailures:    numFailures,
		numRequests:    numRequests,
		ttfbMillis:     ttfbMillis,
		durationMillis: durationMillis,
	}

	for _, serviceName := range services {
		if serviceName == ServiceTypeMgmt ||
			serviceName == ServiceTypeQuery ||
			serviceName == ServiceTypeSearch ||
			serviceName == ServiceTypeAnalytics {
			// ok
		} else {
			logger.Warn("unexpected proxy service name", zap.String("service", string(serviceName)))
		}
	}

	if slices.Contains(services, ServiceTypeMgmt) {
		mux.HandleFunc("/_p/mgmt/", proxy.ProxyMgmt)
	}
	if slices.Contains(services, ServiceTypeQuery) {
		mux.HandleFunc("/_p/query/", proxy.ProxyQuery)
	}
	if slices.Contains(services, ServiceTypeSearch) {
		mux.HandleFunc("/_p/fts/", proxy.ProxySearch)
	}
	if slices.Contains(services, ServiceTypeAnalytics) {
		mux.HandleFunc("/_p/cbas/", proxy.ProxyAnalytics)
	}
	// future: eventing

	return proxy
}

func (p *DataApiProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

func (p *DataApiProxy) writeError(w http.ResponseWriter, err error, msg string) {
	p.logger.Debug(msg, zap.Error(err))

	w.WriteHeader(502)

	if !p.debugMode {
		_, _ = fmt.Fprintf(w, "%s", msg)
	} else {
		_, _ = fmt.Fprintf(w, "%s: %s", msg, err)
	}
}

func (p *DataApiProxy) proxyService(
	w http.ResponseWriter,
	r *http.Request,
	serviceName ServiceType,
) {
	ctx := r.Context()

	stime := time.Now()

	agent, err := p.cbClient.GetClusterAgent(ctx)
	if err != nil {
		p.writeError(w, err, "failed to get agent")
		return
	}

	var roundTripper http.RoundTripper
	var svcEndpoint string
	var pathPrefix string
	switch serviceName {
	case ServiceTypeMgmt:
		endpointInfo, err := agent.GetMgmtEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select mgmt endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/mgmt"
	case ServiceTypeQuery:
		endpointInfo, err := agent.GetQueryEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select query endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/query"
	case ServiceTypeSearch:
		endpointInfo, err := agent.GetSearchEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select search endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/fts"
	case ServiceTypeAnalytics:
		endpointInfo, err := agent.GetAnalyticsEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select analytics endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/cbas"
	default:

		p.writeError(w, nil, "unexpected service name")
		return
	}

	relPath := r.URL.Path[len(pathPrefix):]
	newUrl, _ := url.Parse(svcEndpoint + relPath)
	newUrl.RawQuery = r.URL.RawQuery

	if p.disableAdmin {
		switch serviceName {
		case ServiceTypeMgmt:
			if r.Method != http.MethodGet {
				p.writeError(w, err, "admin endpoints are disabled")
				return
			}
		case ServiceTypeQuery:
			if !strings.HasPrefix(relPath, "/query") {
				p.writeError(w, err, "admin endpoints are disabled")
				return
			}
		case ServiceTypeSearch:
			if !strings.HasPrefix(relPath, "/api/index") &&
				!strings.HasPrefix(relPath, "/api/bucket") {
				p.writeError(w, err, "admin endpoints are disabled")
				return
			}
		case ServiceTypeAnalytics:
			if !strings.HasPrefix(relPath, "/analytics") &&
				!strings.HasPrefix(relPath, "/query") {
				p.writeError(w, err, "admin endpoints are disabled")
				return
			}
		default:
			p.writeError(w, nil, "unexpected service name when performing admin check")
			return
		}
	}

	tp := otel.GetTextMapPropagator()
	ctx = tp.Extract(ctx, propagation.HeaderCarrier(r.Header))

	ctx, span := tracer.Start(ctx, string(serviceName))
	defer span.End()

	proxyReq, err := http.NewRequestWithContext(ctx, r.Method, newUrl.String(), r.Body)
	if err != nil {
		p.writeError(w, err, "failed to create proxy request")
		return
	}

	// copy some other details
	proxyReq.Header = r.Header

	tr := otelhttp.NewTransport(
		roundTripper,
		// By setting the otelhttptrace client in this transport, it can be
		// injected into the context after the span is started, which makes the
		// httptrace spans children of the transport one.
		otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace {
			return otelhttptrace.NewClientTrace(ctx)
		}),
	)

	proxyResp, err := tr.RoundTrip(proxyReq)
	if err != nil {
		p.numFailures.Add(ctx, 1, metric.WithAttributes(
			attribute.String("service_name", string(serviceName)),
		))
		p.writeError(w, err, "failed to receive response")
		return
	}

	ttfbEtime := time.Now()
	ttfbDtime := ttfbEtime.Sub(stime)
	ttfbMillis := ttfbDtime.Milliseconds()

	p.ttfbMillis.Record(ctx, ttfbMillis, metric.WithAttributes(
		attribute.String("service_name", string(serviceName)),
		attribute.String("http_status_code",
			fmt.Sprintf("%dxx", proxyResp.StatusCode/100)),
	))

	for k, vs := range proxyResp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(proxyResp.StatusCode)
	_, err = io.Copy(w, proxyResp.Body)
	if err != nil {
		// we cannot actually write an error here as we've already started writing the response
		p.logger.Debug("failed to write response body", zap.Error(err))
		return
	}

	etime := time.Now()
	dtime := etime.Sub(stime)
	dtimeMillis := dtime.Milliseconds()

	p.durationMillis.Record(ctx, dtimeMillis, metric.WithAttributes(
		attribute.String("service_name", string(serviceName)),
		attribute.String("http_status_code",
			fmt.Sprintf("%dxx", proxyResp.StatusCode/100)),
	))

	p.numRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("service_name", string(serviceName)),
		attribute.String("http_status_code",
			fmt.Sprintf("%dxx", proxyResp.StatusCode/100)),
	))
}

func (p *DataApiProxy) ProxyMgmt(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, ServiceTypeMgmt)
}

func (p *DataApiProxy) ProxyQuery(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, ServiceTypeQuery)
}

func (p *DataApiProxy) ProxySearch(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, ServiceTypeSearch)
}

func (p *DataApiProxy) ProxyAnalytics(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, ServiceTypeAnalytics)
}
