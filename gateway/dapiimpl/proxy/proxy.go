package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"

	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
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
}

func NewDataApiProxy(
	logger *zap.Logger,
	cbClient *gocbcorex.BucketsTrackingAgentManager,
	services []ServiceType,
	debugMode bool,
) *DataApiProxy {
	mux := http.NewServeMux()

	proxy := &DataApiProxy{
		logger:    logger,
		cbClient:  cbClient,
		debugMode: debugMode,
		mux:       mux,
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
		p.writeError(w, err, "failed to receive response")
		return
	}

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
