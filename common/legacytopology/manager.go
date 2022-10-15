package legacytopology

import (
	"context"
	"log"

	"github.com/couchbase/stellar-nebula/common/nebclustering"
	"github.com/couchbase/stellar-nebula/common/remotetopology"
	"github.com/couchbase/stellar-nebula/common/topologyutils"
)

type ManagerOptions struct {
	LocalTopologyProvider  nebclustering.Provider
	RemoteTopologyProvider remotetopology.Provider
}

type Manager struct {
	localTopologyProvider  nebclustering.Provider
	remoteTopologyProvider remotetopology.Provider
}

var _ Provider = (*Manager)(nil)

func NewManager(opts *ManagerOptions) (*Manager, error) {
	return &Manager{
		localTopologyProvider:  opts.LocalTopologyProvider,
		remoteTopologyProvider: opts.RemoteTopologyProvider,
	}, nil
}

func (m *Manager) Watch(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	topologyCh, err := topologyutils.TopologyMergeWatch(ctx, m.localTopologyProvider, m.remoteTopologyProvider, bucketName)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Topology)
	go func() {
		for topology := range topologyCh {
			newTopology, err := ComputeTopology(topology.Local, topology.Remote)
			if err != nil {
				log.Printf("failed to compute ps topology: %s", err)
				continue
			}

			outputCh <- newTopology
		}
		close(outputCh)
	}()

	return outputCh, nil
}
