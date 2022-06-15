package server

import (
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/protos"
)

type couchbaseRoutingServer struct {
	protos.UnimplementedCouchbaseRoutingServer

	topologyManager *TopologyManager
}

func (s *couchbaseRoutingServer) WatchRouting(in *protos.WatchRoutingRequest, out protos.CouchbaseRouting_WatchRoutingServer) error {
	// TODO(brett19): Implement proper topology updates...
	// For now we just fill out the entiry topology such that all the endpoints
	// point back to this singular node which can handle all request types.  Note
	// that we do not generate a vbucket map at the moment.

topologyLoop:
	for {
		topology := s.topologyManager.GetTopology()

		err := out.Send(&protos.WatchRoutingResponse{
			Endpoints: topology.Endpoints,
			KvRouting: &protos.WatchRoutingResponse_VbucketRouting{
				VbucketRouting: &protos.VbucketMapRouting{
					Endpoints: topology.Endpoints,
					Vbuckets:  nil,
				},
			},
			QueryRouting: &protos.QueryRouting{
				Endpoints: topology.Endpoints,
			},
			SearchQueryRouting: &protos.SearchQueryRouting{
				Endpoints: topology.Endpoints,
			},
			AnalyticsQueryRouting: &protos.AnalyticsQueryRouting{
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

func NewCouchbaseRoutingServer(topologyManager *TopologyManager) *couchbaseRoutingServer {
	return &couchbaseRoutingServer{
		topologyManager: topologyManager,
	}
}
