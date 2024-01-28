// This file is to handle things such as metrics/health/pprof, etc

package webapi

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type WebServerOptions struct {
	Logger        *zap.Logger
	LogLevel      *zap.AtomicLevel
	ListenAddress string
}

type WebServer struct {
	logger        *zap.Logger
	logLevel      *zap.AtomicLevel
	listenAddress string
	httpServer    *http.Server
	isHealthy     atomic.Bool
}

func newWebServer(opts WebServerOptions) *WebServer {
	return &WebServer{
		logger:        opts.Logger,
		logLevel:      opts.LogLevel,
		listenAddress: opts.ListenAddress,
	}
}

func (w *WebServer) handleRoot(rw http.ResponseWriter, r *http.Request) {
	rw.WriteHeader(200)
	_, err := rw.Write([]byte("cloud native gateway webapi"))
	if err != nil {
		w.logger.Debug("failed to write generic root response", zap.Error(err))
	}
}

func (w *WebServer) handleLive(rw http.ResponseWriter, r *http.Request) {
	rw.WriteHeader(200)
	_, _ = rw.Write([]byte("ok"))
}

func (w *WebServer) handleReady(rw http.ResponseWriter, r *http.Request) {
	if w.isHealthy.Load() {
		rw.WriteHeader(200)
		_, _ = rw.Write([]byte("ok"))
	} else {
		rw.WriteHeader(503)
		_, _ = rw.Write([]byte("not ok"))
	}
}

func (w *WebServer) SetHealth(isHealthy bool) {
	oldIsHealthy := w.isHealthy.Swap(isHealthy)
	if oldIsHealthy != isHealthy {
		if isHealthy {
			w.logger.Info("system health marked as healthy")
		} else {
			w.logger.Info("system health marked as unhealthy")
		}
	}
}

func (w *WebServer) ListenAndServe() error {
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())
	r.HandleFunc("/health", w.handleReady)
	r.HandleFunc("/live", w.handleLive)
	r.HandleFunc("/ready", w.handleReady)
	r.HandleFunc("/", w.handleRoot)

	w.httpServer = &http.Server{
		Handler:      r,
		Addr:         w.listenAddress,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	w.logger.Info("starting health/metrics server",
		zap.String("listenAddress", w.listenAddress))

	return w.httpServer.ListenAndServe()
}

var globalWebLock sync.Mutex
var globalWebServer *WebServer = nil

func InitializeWebServer(opts WebServerOptions) {
	globalWebLock.Lock()
	if globalWebServer != nil {
		globalWebLock.Unlock()
		return
	}

	globalWebServer = newWebServer(opts)
	globalWebLock.Unlock()
	go func() {
		err := globalWebServer.ListenAndServe()
		if err != nil {
			opts.Logger.Error("Failed to listen and serve web server", zap.Error(err))
		}
	}()
}

func MarkSystemHealthy() {
	if globalWebServer == nil {
		return
	}

	globalWebServer.SetHealth(true)
}

func MarkSystemUnhealthy() {
	if globalWebServer == nil {
		return
	}

	globalWebServer.SetHealth(false)
}
