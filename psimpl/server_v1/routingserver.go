package server_v1

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/common/psclustering"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
)

type RoutingServer struct {
	routing_v1.UnimplementedRoutingServer

	clusteringManager *psclustering.Manager
}

func (s *RoutingServer) WatchRouting(in *routing_v1.WatchRoutingRequest, out routing_v1.Routing_WatchRoutingServer) error {
topologyLoop:
	for {
		topology, err := s.clusteringManager.Get(out.Context())
		if err != nil {
			return err
		}

		var endpoints []*routing_v1.RoutingEndpoint
		for _, endpoint := range topology.Members {
			endpoints = append(endpoints, &routing_v1.RoutingEndpoint{
				Address: fmt.Sprintf("%s:%d", endpoint.AdvertiseAddr, endpoint.AdvertisePort),
			})
		}

		err = out.Send(&routing_v1.WatchRoutingResponse{
			Endpoints: endpoints,
		})
		if err != nil {
			log.Printf("failed to send topology update: %s", err)
		}

		select {
		case <-time.After(15 * time.Second):
			// we send toplogy updates every 15 seconds for demo purposes
		case <-out.Context().Done():
			break topologyLoop
		}
	}

	return nil
}

func NewRoutingServer(clusteringManager *psclustering.Manager) *RoutingServer {
	return &RoutingServer{
		clusteringManager: clusteringManager,
	}
}
