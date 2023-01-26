// This file is to handle things such as metrics/health/pprof, etc

package webapi

import (
	"net/http"
	"sync"
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
	_, err := rw.Write([]byte("Welcome to the stellar nebula internal webapi"))
	if err != nil {
		w.logger.Debug("failed to write generic root response", zap.Error(err))
	}
}

func (w *WebServer) ListenAndServe() error {
	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())
	r.HandleFunc("/", w.handleRoot)

	w.httpServer = &http.Server{
		Handler:      r,
		Addr:         w.listenAddress,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

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
