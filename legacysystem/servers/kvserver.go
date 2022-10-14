package servers

import (
	"net"
	"strings"
	"sync"

	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"go.uber.org/zap"
)

type KvServerOptions struct {
	Logger        *zap.Logger
	DataServer    data_v1.DataServer
	RoutingServer routing_v1.RoutingServer
}

type KvServer struct {
	logger        *zap.Logger
	dataServer    data_v1.DataServer
	routingServer routing_v1.RoutingServer

	lock    sync.Mutex
	clients []*KvServerClient
}

func NewKvServer(opts *KvServerOptions) (*KvServer, error) {
	server := &KvServer{
		logger:        opts.Logger,
		dataServer:    opts.DataServer,
		routingServer: opts.RoutingServer,
	}

	return server, nil
}

func (s *KvServer) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			// Accept() returns an error with substring "use of closed network connection" if
			// the socket has been closed elsewhere (ie. during graceful stop, instead of EOF).
			// Go is "supporting" this string comparison until an alternative is presented to
			// improve the situation. See https://github.com/golang/go/issues/4373 for info.
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}

			s.logger.Error("failed to accept client", zap.Error(err))
			break
		}

		s.handleNewConnection(conn)
	}

	err := l.Close()
	if err != nil {
		s.logger.Error("failed to close kv server listener", zap.Error(err))
	}

	return nil
}

func (s *KvServer) handleNewConnection(conn net.Conn) {
	s.logger.Info("new kv client connected",
		zap.Stringer("sourceaddress", conn.LocalAddr()),
	)

	client, err := NewKvServerClient(&KvServerClientOptions{
		Logger: s.logger.With(
			zap.Stringer("address", conn.RemoteAddr()),
		),
		ParentServer:  s,
		DataServer:    s.dataServer,
		RoutingServer: s.routingServer,
		Conn:          conn,
	})
	if err != nil {
		s.logger.Info("failed to init kv proxy client", zap.Error(err))
		conn.Close()
		return
	}

	s.lock.Lock()
	s.clients = append(s.clients, client)
	s.lock.Unlock()
}

func (s *KvServer) handleClientDisconnect(client *KvServerClient) {
	s.lock.Lock()

	foundClientIdx := -1
	for iterIdx, iterClient := range s.clients {
		if iterClient == client {
			foundClientIdx = iterIdx
			break
		}
	}

	if foundClientIdx == -1 {
		// TODO(brett19): Don't panic, everything is going to be alright....
		panic("attempted to remove a client that did not exist")
	}

	s.clients[foundClientIdx] = s.clients[len(s.clients)-1]
	s.clients = s.clients[:len(s.clients)-1]

	s.lock.Unlock()
}
