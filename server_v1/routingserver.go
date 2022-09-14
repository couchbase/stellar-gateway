package server_v1

import (
	"log"
	"time"

	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
	"github.com/couchbase/stellar-nebula/topology"
)

type routingServer struct {
	routing_v1.UnimplementedRoutingServer

	topologyManager *topology.TopologyManager
}

func (s *routingServer) WatchRouting(in *routing_v1.WatchRoutingRequest, out routing_v1.Routing_WatchRoutingServer) error {
	// TODO(brett19): Implement proper topology updates...
	// For now we just fill out the entiry topology such that all the endpoints
	// point back to this singular node which can handle all request types.  Note
	// that we do not generate a vbucket map at the moment.

topologyLoop:
	for {
		topology := s.topologyManager.GetTopology()

		err := out.Send(&routing_v1.WatchRoutingResponse{
			Endpoints: topology.Endpoints,
			KvRouting: &routing_v1.WatchRoutingResponse_VbucketRouting{
				VbucketRouting: &routing_v1.VbucketMapRouting{
					Endpoints: topology.Endpoints,
					Vbuckets:  nil,
				},
			},
			QueryRouting: &routing_v1.QueryRouting{
				Endpoints: topology.Endpoints,
			},
			SearchQueryRouting: &routing_v1.SearchQueryRouting{
				Endpoints: topology.Endpoints,
			},
			AnalyticsQueryRouting: &routing_v1.AnalyticsQueryRouting{
				Endpoints: topology.Endpoints,
			},
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

func NewRoutingServer(topologyManager *topology.TopologyManager) *routingServer {
	return &routingServer{
		topologyManager: topologyManager,
	}
}
