package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
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
	debugMode bool,
) *DataApiProxy {
	mux := http.NewServeMux()

	proxy := &DataApiProxy{
		logger:    logger,
		cbClient:  cbClient,
		debugMode: debugMode,
		mux:       mux,
	}

	mux.HandleFunc("/proxy/mgmt/", proxy.ProxyMgmt)
	mux.HandleFunc("/proxy/query/", proxy.ProxyQuery)
	mux.HandleFunc("/proxy/search/", proxy.ProxySearch)
	mux.HandleFunc("/proxy/analytics/", proxy.ProxyAnalytics)

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
	serviceName string,
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
	if serviceName == "mgmt" {
		endpointInfo, err := agent.GetMgmtEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select mgmt endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/proxy/mgmt"
	} else if serviceName == "query" {
		endpointInfo, err := agent.GetQueryEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select query endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/proxy/query"
	} else if serviceName == "search" {
		endpointInfo, err := agent.GetSearchEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select search endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/proxy/search"
	} else if serviceName == "analytics" {
		endpointInfo, err := agent.GetAnalyticsEndpoint(ctx)
		if err != nil {
			p.writeError(w, err, "failed to select analytics endpoint")
			return
		}

		roundTripper = endpointInfo.RoundTripper
		svcEndpoint = endpointInfo.Endpoint
		pathPrefix = "/proxy/analytics"
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
	p.proxyService(w, r, "mgmt")
}

func (p *DataApiProxy) ProxyQuery(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, "query")
}

func (p *DataApiProxy) ProxySearch(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, "search")
}

func (p *DataApiProxy) ProxyAnalytics(w http.ResponseWriter, r *http.Request) {
	p.proxyService(w, r, "analytics")
}
