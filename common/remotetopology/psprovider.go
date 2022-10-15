package remotetopology

import (
	"context"
	"log"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/couchbase/stellar-nebula/client"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
)

type PSProviderOptions struct {
	Client *client.RoutingClient
}

type PSProvider struct {
	client *client.RoutingClient
}

func NewPSProvider(opts *PSProviderOptions) (*PSProvider, error) {
	return &PSProvider{
		client: opts.Client,
	}, nil
}

func (p *PSProvider) translateTopology(t *routing_v1.WatchRoutingResponse) *Topology {
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

func (p *PSProvider) watchTopology(ctx context.Context, bucketName *string) (<-chan *Topology, error) {
	// TODO(brett19): PSProvider.watchTopology is something that probably belongs in the client.
	// Putting it in the client will be helpful because it's likely used in multipled places, and the
	// client itself needs to maintain knowledge of the topology anyways, we can perform coalescing
	// of those watchers in the client (to reduce the number of watchers).

	b := backoff.NewExponentialBackOff()
	b.Reset()

	routingStream, err := p.client.RoutingV1().WatchRouting(ctx, &routing_v1.WatchRoutingRequest{
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
			routingStream, err := p.client.RoutingV1().WatchRouting(ctx, &routing_v1.WatchRoutingRequest{
				BucketName: bucketName,
			})
			if err != nil {
				// TODO(brett19): Implement better error handling here...
				log.Printf("failed to watch routing: %s", err)

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
					log.Printf("failed to recv updated topology: %s", err)
					break
				}

				outputCh <- p.translateTopology(routingResp)
			}
		}
	}()

	return outputCh, nil
}

func (p *PSProvider) Watch(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	// TODO(brett19): Remove this pointer shenanigans
	var bucketNamePtr *string
	if bucketName != "" {
		bucketNamePtr = &bucketName
	}
	return p.watchTopology(ctx, bucketNamePtr)
}
