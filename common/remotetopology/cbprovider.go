package remotetopology

import (
	"context"

	"github.com/couchbase/stellar-nebula/common/cbtopology"
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

func (p *CBProvider) translateClusterTopology(t *cbtopology.Topology) *Topology {
	nodes := make([]*Node, len(t.Nodes))
	for cbNodeIdx, cbNode := range t.Nodes {
		node := &Node{
			NodeID:      cbNode.NodeID,
			ServerGroup: cbNode.ServerGroup,
		}
		nodes[cbNodeIdx] = node
	}

	return &Topology{
		Revision: []uint64{t.Revision, t.RevEpoch},
		Nodes:    nodes,
	}
}

func (p *CBProvider) translateBucketTopology(t *cbtopology.BucketTopology) *Topology {
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

	dataNodes := make([]*DataNode, len(t.DataNodes))
	for cbDataNodeIdx, cbDataNode := range t.DataNodes {
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

	return &Topology{
		Revision: []uint64{t.Revision, t.RevEpoch},
		Nodes:    nodes,
		VbucketRouting: &VbucketRouting{
			NumVbuckets: t.NumVbuckets,
			Nodes:       dataNodes,
		},
	}
}

func (p *CBProvider) Watch(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	// TODO(brett19): We should just merge the Watch functions in this provider...
	if bucketName == "" {
		cbTopologyCh, err := p.provider.WatchCluster(ctx)
		if err != nil {
			return nil, err
		}

		outputCh := make(chan *Topology)
		go func() {
			for cbTopology := range cbTopologyCh {
				outputCh <- p.translateClusterTopology(cbTopology)
			}
		}()
		return outputCh, err
	} else {
		cbTopologyCh, err := p.provider.WatchBucket(ctx, bucketName)
		if err != nil {
			return nil, err
		}

		outputCh := make(chan *Topology)
		go func() {
			for cbTopology := range cbTopologyCh {
				outputCh <- p.translateBucketTopology(cbTopology)
			}
			close(outputCh)
		}()

		return outputCh, err
	}
}
