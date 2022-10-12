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

func (p *PSProvider) parseRevision(psRevision []uint64) (uint64, uint64) {
	var revEpoch, revision uint64
	if len(psRevision) >= 2 {
		revEpoch = psRevision[0]
		revision = psRevision[1]
	} else if len(psRevision) >= 1 {
		revEpoch = 0
		revision = psRevision[0]
	}
	return revEpoch, revision
}

func (p *PSProvider) translateClusterTopology(t *routing_v1.WatchRoutingResponse) *Topology {
	nodes := make([]*Node, len(t.Endpoints))
	for psEpIdx, psEp := range t.Endpoints {
		node := &Node{
			NodeID:      psEp.Id,
			ServerGroup: psEp.ServerGroup,
		}
		nodes[psEpIdx] = node
	}

	revEpoch, revision := p.parseRevision(t.Revision)

	return &Topology{
		RevEpoch: revEpoch,
		Revision: revision,
		Nodes:    nodes,
	}
}

func (p *PSProvider) translateBucketTopology(t *routing_v1.WatchRoutingResponse) *BucketTopology {
	nodes := make([]*Node, len(t.Endpoints))
	for psEpIdx, psEp := range t.Endpoints {
		node := &Node{
			NodeID:      psEp.Id,
			ServerGroup: psEp.ServerGroup,
		}
		nodes[psEpIdx] = node
	}

	var dataNodes []*DataNode
	vbRouting := t.GetVbucketDataRouting()
	if vbRouting != nil {
		dataNodes := make([]*DataNode, len(vbRouting.Endpoints))
		for psDataEpIdx, psDataEp := range vbRouting.Endpoints {
			dataNode := &DataNode{
				Node:          nodes[psDataEp.EndpointIdx],
				LocalVbuckets: psDataEp.LocalVbuckets,
				GroupVbuckets: psDataEp.GroupVbuckets,
			}
			dataNodes[psDataEpIdx] = dataNode
		}
	}

	revEpoch, revision := p.parseRevision(t.Revision)

	return &BucketTopology{
		RevEpoch:  revEpoch,
		Revision:  revision,
		Nodes:     nodes,
		DataNodes: dataNodes,
	}
}

func (p *PSProvider) watchTopology(ctx context.Context, bucketName *string) (<-chan *routing_v1.WatchRoutingResponse, error) {
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

	outputCh := make(chan *routing_v1.WatchRoutingResponse)
	go func() {
		outputCh <- routingResp

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

				outputCh <- routingResp
			}
		}
	}()

	return outputCh, nil
}

func (p *PSProvider) WatchCluster(ctx context.Context) (<-chan *Topology, error) {
	routingStream, err := p.watchTopology(ctx, nil)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Topology)
	go func() {
		for cbTopology := range routingStream {
			outputCh <- p.translateClusterTopology(cbTopology)
		}
		close(outputCh)
	}()

	return outputCh, err
}

func (p *PSProvider) WatchBucket(ctx context.Context, bucketName string) (<-chan *BucketTopology, error) {
	routingStream, err := p.watchTopology(ctx, &bucketName)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *BucketTopology)
	go func() {
		for cbTopology := range routingStream {
			outputCh <- p.translateBucketTopology(cbTopology)
		}
		close(outputCh)
	}()

	return outputCh, err
}
