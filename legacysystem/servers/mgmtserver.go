package servers

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type MgmtServerOptions struct {
	Logger *zap.Logger

	BindAddress string
	BindPort    int
	TlsConfig   *tls.Config

	RoutingServer routing_v1.RoutingServer
}

type MgmtServer struct {
	logger        *zap.Logger
	bindAddress   string
	bindPort      int
	tlsConfig     *tls.Config
	routingServer routing_v1.RoutingServer
}

func NewMgmtServer(opts *MgmtServerOptions) (*MgmtServer, error) {
	s := &MgmtServer{
		logger:        opts.Logger,
		bindAddress:   opts.BindAddress,
		bindPort:      opts.BindPort,
		tlsConfig:     opts.TlsConfig,
		routingServer: opts.RoutingServer,
	}

	err := s.init()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *MgmtServer) init() error {
	router := mux.NewRouter()

	router.HandleFunc("/", s.handleRoot)

	listenAddress := fmt.Sprintf("%s:%d", s.bindAddress, s.bindPort)
	httpServer := &http.Server{
		Addr:      listenAddress,
		Handler:   router,
		TLSConfig: s.tlsConfig,
	}

	// TODO(brett19): Need better error handling for mgmt servers
	if s.tlsConfig != nil {
		go httpServer.ListenAndServeTLS("", "")
	} else {
		go httpServer.ListenAndServe()
	}

	return nil
}

func (s *MgmtServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	// TODO(brett19): Handle errors and stuff here...
	w.WriteHeader(200)
	w.Write([]byte("mgmt service"))
}
