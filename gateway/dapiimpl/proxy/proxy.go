package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/couchbase/gocbcorex"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var (
	meter = otel.Meter("github.com/couchbase/stellar-gateway/gateway/dapiimpl/proxy")
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
	logger    *zap.Logger
	cbClient  *gocbcorex.BucketsTrackingAgentManager
	debugMode bool
	mux       *http.ServeMux

	numFailures    metric.Int64Counter
	numRequests    metric.Int64Counter
	ttfbMillis     metric.Int64Histogram
	durationMillis metric.Int64Histogram
}

func NewDataApiProxy(
	logger *zap.Logger,
	cbClient *gocbcorex.BucketsTrackingAgentManager,
	services []ServiceType,
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
		fmt.Fprintf(w, "%s", msg)
	} else {
		fmt.Fprintf(w, "%s: %s", msg, err)
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
	if serviceName == ServiceTypeMgmt {
		endpointInfo, err := agent.GetMgmtEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select mgmt endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/mgmt"
	} else if serviceName == ServiceTypeQuery {
		endpointInfo, err := agent.GetQueryEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select query endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/query"
	} else if serviceName == ServiceTypeSearch {
		endpointInfo, err := agent.GetSearchEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select search endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/fts"
	} else if serviceName == ServiceTypeAnalytics {
		endpointInfo, err := agent.GetAnalyticsEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select analytics endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/_p/cbas"
	} else {
		p.writeError(w, nil, "unexpected service name")
		return
	}

	relPath := r.URL.Path[len(pathPrefix):]
	newUrl, _ := url.Parse(svcEndpoint + relPath)
	newUrl.RawQuery = r.URL.RawQuery

	proxyReq, err := http.NewRequest(r.Method, newUrl.String(), r.Body)
	if err != nil {
		p.writeError(w, err, "failed to create proxy request")
		return
	}

	// copy some other details
	proxyReq.Header = r.Header

	proxyResp, err := roundTripper.RoundTrip(proxyReq)
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
