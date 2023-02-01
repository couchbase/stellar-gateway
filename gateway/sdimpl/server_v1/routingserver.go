package server_v1

import (
	"fmt"
	"log"

	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/utils/latestonlychannel"
)

type RoutingServer struct {
	routing_v1.UnimplementedRoutingServer

	topologyProvider topology.Provider
}

func NewRoutingServer(topologyProvider topology.Provider) *RoutingServer {
	return &RoutingServer{
		topologyProvider: topologyProvider,
	}
}

func (s *RoutingServer) WatchRouting(in *routing_v1.WatchRoutingRequest, out routing_v1.Routing_WatchRoutingServer) error {
	topologyCh, err := s.topologyProvider.Watch(out.Context(), in.GetBucketName())
	if err != nil {
		return err
	}

	// we wrap the topology channel in a helper which ensures we can block to send
	// topologies without blocking the channel itself...
	topologyCh = latestonlychannel.Wrap(topologyCh)

	for topology := range topologyCh {
		var endpoints []*routing_v1.RoutingEndpoint
		for _, node := range topology.Nodes {
			endpoints = append(endpoints, &routing_v1.RoutingEndpoint{
				Address: fmt.Sprintf("%s:%d", node.Address, node.Port),
			})
		}

		err = out.Send(&routing_v1.WatchRoutingResponse{
			Endpoints: endpoints,
		})
		if err != nil {
			log.Printf("failed to send topology update: %s", err)
		}
	}

	return nil
}
