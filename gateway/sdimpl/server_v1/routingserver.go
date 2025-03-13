/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"fmt"

	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"github.com/couchbase/stellar-gateway/utils/latestonlychannel"
	"go.uber.org/zap"
)

type RoutingServer struct {
	routing_v1.UnimplementedRoutingServiceServer
	logger           *zap.Logger
	topologyProvider topology.Provider
}

func NewRoutingServer(topologyProvider topology.Provider, logger *zap.Logger) *RoutingServer {
	return &RoutingServer{
		topologyProvider: topologyProvider,
		logger:           logger,
	}
}

func (s *RoutingServer) WatchRouting(in *routing_v1.WatchRoutingRequest, out routing_v1.RoutingService_WatchRoutingServer) error {
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
			s.logger.Error("failed to send topology update", zap.Error(err))
		}
	}

	return nil
}
