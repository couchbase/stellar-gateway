/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package servers

import (
	"net"
	"net/http"

	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type QueryServerOptions struct {
	Logger *zap.Logger

	QueryClient query_v1.QueryServiceClient
}

type QueryServer struct {
	logger      *zap.Logger
	queryClient query_v1.QueryServiceClient

	httpServer *http.Server
}

func NewQueryServer(opts *QueryServerOptions) (*QueryServer, error) {
	s := &QueryServer{
		logger:      opts.Logger,
		queryClient: opts.QueryClient,
	}

	router := mux.NewRouter()

	router.HandleFunc("/", s.handleRoot)

	s.httpServer = &http.Server{
		Handler: router,
	}

	return s, nil
}

func (s *QueryServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	// TODO(brett19): Handle errors and stuff here...
	w.WriteHeader(200)
	_, err := w.Write([]byte("query service"))
	if err != nil {
		s.logger.Debug("failed to write root response", zap.Error(err))
	}
}

func (s *QueryServer) Serve(l net.Listener) error {
	return s.httpServer.Serve(l)
}
