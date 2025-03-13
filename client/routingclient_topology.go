/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package client

import (
	"context"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"go.uber.org/zap"
)

func (p *RoutingClient) translateTopology(t *routing_v1.WatchRoutingResponse) *Topology {
	nodes := make([]*Node, len(t.Endpoints))
	for psEpIdx, psEp := range t.Endpoints {
		node := &Node{
			NodeID:      psEp.Id,
			ServerGroup: psEp.ServerGroup,
		}
		nodes[psEpIdx] = node
	}

	var vbRouting *VbucketRouting
	psVbRouting := t.GetVbucketDataRouting()
	if vbRouting != nil {
		dataNodes := make([]*DataNode, len(psVbRouting.Endpoints))
		for psDataEpIdx, psDataEp := range psVbRouting.Endpoints {
			dataNode := &DataNode{
				Node:          nodes[psDataEp.EndpointIdx],
				LocalVbuckets: psDataEp.LocalVbuckets,
				GroupVbuckets: psDataEp.GroupVbuckets,
			}
			dataNodes[psDataEpIdx] = dataNode
		}
		vbRouting = &VbucketRouting{
			NumVbuckets: uint(vbRouting.NumVbuckets),
			Nodes:       dataNodes,
		}
	}

	return &Topology{
		Revision:       t.Revision,
		Nodes:          nodes,
		VbucketRouting: vbRouting,
	}
}

func (p *RoutingClient) watchTopology(ctx context.Context, bucketName *string) (<-chan *Topology, error) {
	// TODO(brett19): PSProvider.watchTopology is something that probably belongs in the client.
	// Putting it in the client will be helpful because it's likely used in multipled places, and the
	// client itself needs to maintain knowledge of the topology anyways, we can perform coalescing
	// of those watchers in the client (to reduce the number of watchers).

	b := backoff.NewExponentialBackOff()
	b.Reset()

	routingStream, err := p.RoutingV1().WatchRouting(ctx, &routing_v1.WatchRoutingRequest{
		BucketName: bucketName,
	})
	if err != nil {
		return nil, err
	}

	routingResp, err := routingStream.Recv()
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Topology)
	go func() {
		outputCh <- p.translateTopology(routingResp)

	MainLoop:
		for {
			routingStream, err := p.RoutingV1().WatchRouting(ctx, &routing_v1.WatchRoutingRequest{
				BucketName: bucketName,
			})
			if err != nil {
				// TODO(brett19): Implement better error handling here...
				p.logger.Error("failed to watch routing", zap.Error(err))

				select {
				case <-time.After(b.NextBackOff()):
					continue
				case <-ctx.Done():
					break MainLoop
				}
			}

			for {
				routingResp, err := routingStream.Recv()
				if err != nil {
					p.logger.Error("failed to recv updated topology", zap.Error(err))
					break
				}

				outputCh <- p.translateTopology(routingResp)
			}
		}
	}()

	return outputCh, nil
}

func (p *RoutingClient) WatchTopology(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	// TODO(brett19): Remove this pointer shenanigans
	var bucketNamePtr *string
	if bucketName != "" {
		bucketNamePtr = &bucketName
	}
	return p.watchTopology(ctx, bucketNamePtr)
}
