package server_v1

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/common/topology"
	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
)

type routingServer struct {
	routing_v1.UnimplementedRoutingServer

	topologyProvider topology.Provider
}

func (s *routingServer) WatchRouting(in *routing_v1.WatchRoutingRequest, out routing_v1.Routing_WatchRoutingServer) error {
topologyLoop:
	for {
		topology, err := s.topologyProvider.Get()
		if err != nil {
			return err
		}

		var endpoints []*routing_v1.RoutingEndpoint
		for _, endpoint := range topology.Endpoints {
			endpoints = append(endpoints, &routing_v1.RoutingEndpoint{
				Endpoint: fmt.Sprintf("%s:%d", endpoint.AdvertiseAddr, endpoint.AdvertisePort),
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

func NewRoutingServer(topologyProvider topology.Provider) *routingServer {
	return &routingServer{
		topologyProvider: topologyProvider,
	}
}
