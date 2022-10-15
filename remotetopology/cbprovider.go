package remotetopology

import (
	"context"

	"github.com/couchbase/stellar-nebula/contrib/cbtopology"
)

type CBProviderOptions struct {
	Provider cbtopology.Provider
}

type CBProvider struct {
	provider cbtopology.Provider
}

func NewCBProvider(opts *CBProviderOptions) (*CBProvider, error) {
	return &CBProvider{
		provider: opts.Provider,
	}, nil
}

func (p *CBProvider) translateTopology(t *cbtopology.Topology) *Topology {
	nodes := make([]*Node, len(t.Nodes))
	nodesMap := make(map[*cbtopology.Node]*Node)
	for cbNodeIdx, cbNode := range t.Nodes {
		node := &Node{
			NodeID:      cbNode.NodeID,
			ServerGroup: cbNode.ServerGroup,
		}
		nodes[cbNodeIdx] = node
		nodesMap[cbNode] = node
	}

	var vbucketRouting *VbucketRouting
	if t.VbucketMapping != nil {
		dataNodes := make([]*DataNode, len(t.VbucketMapping.Nodes))
		for cbDataNodeIdx, cbDataNode := range t.VbucketMapping.Nodes {
			var localVbuckets, groupVbuckets []uint32

			// We directly copy the local vbuckets from the couchbase config
			localVbuckets = make([]uint32, len(cbDataNode.Vbuckets))
			for vbIdx, vbId := range cbDataNode.Vbuckets {
				localVbuckets[vbIdx] = uint32(vbId)
			}

			// We don't provide GroupVbuckets here, because technically the service
			// under the hood (kv_engine), isn't capable of contacting it's group.
			groupVbuckets = nil

			dataNode := &DataNode{
				Node:          nodesMap[cbDataNode.Node],
				LocalVbuckets: localVbuckets,
				GroupVbuckets: groupVbuckets,
			}
			dataNodes[cbDataNodeIdx] = dataNode
		}

		vbucketRouting = &VbucketRouting{
			NumVbuckets: t.VbucketMapping.NumVbuckets,
			Nodes:       dataNodes,
		}
	}

	return &Topology{
		Revision:       []uint64{t.Revision, t.RevEpoch},
		Nodes:          nodes,
		VbucketRouting: vbucketRouting,
	}
}

func (p *CBProvider) Watch(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	cbTopologyCh, err := p.provider.Watch(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Topology)
	go func() {
		for cbTopology := range cbTopologyCh {
			outputCh <- p.translateTopology(cbTopology)
		}
		close(outputCh)
	}()

	return outputCh, err
}
