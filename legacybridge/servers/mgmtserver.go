package servers

import (
	"net"
	"net/http"

	"github.com/couchbase/stellar-nebula/legacybridge/topology"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type MgmtServerOptions struct {
	Logger *zap.Logger

	TopologyProvider topology.Provider
}

type MgmtServer struct {
	logger           *zap.Logger
	topologyProvider topology.Provider

	httpServer *http.Server
}

func NewMgmtServer(opts *MgmtServerOptions) (*MgmtServer, error) {
	s := &MgmtServer{
		logger:           opts.Logger,
		topologyProvider: opts.TopologyProvider,
	}

	router := mux.NewRouter()

	router.HandleFunc("/", s.handleRoot)

	s.httpServer = &http.Server{
		Handler: router,
	}

	return s, nil
}

func (s *MgmtServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	// TODO(brett19): Handle errors and stuff here...
	w.WriteHeader(200)
	w.Write([]byte("mgmt service"))
}

func (s *MgmtServer) Serve(l net.Listener) error {
	return s.httpServer.Serve(l)
}
