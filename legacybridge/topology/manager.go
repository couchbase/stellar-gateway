package topology

import (
	"context"
	"log"

	"github.com/couchbase/stellar-gateway/client"
	"github.com/couchbase/stellar-gateway/legacybridge/clustering"
	"github.com/couchbase/stellar-gateway/utils/channelmerge"
)

type RemoteTopologyProvider interface {
	WatchTopology(ctx context.Context, bucketName string) (<-chan *client.Topology, error)
}

var _ RemoteTopologyProvider = (*client.RoutingClient)(nil)

type ManagerOptions struct {
	LocalTopologyProvider  clustering.Provider
	RemoteTopologyProvider RemoteTopologyProvider
}

type Manager struct {
	localTopologyProvider  clustering.Provider
	remoteTopologyProvider RemoteTopologyProvider
}

var _ Provider = (*Manager)(nil)

func NewManager(opts *ManagerOptions) (*Manager, error) {
	return &Manager{
		localTopologyProvider:  opts.LocalTopologyProvider,
		remoteTopologyProvider: opts.RemoteTopologyProvider,
	}, nil
}

func (m *Manager) Watch(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)

	clusterCh, err := m.localTopologyProvider.Watch(cancelCtx)
	if err != nil {
		cancelFn()
		return nil, err
	}

	remoteCh, err := m.remoteTopologyProvider.WatchTopology(cancelCtx, bucketName)
	if err != nil {
		cancelFn()
		return nil, err
	}

	topologyCh := channelmerge.Merge(clusterCh, remoteCh)

	outputCh := make(chan *Topology)
	go func() {
		for topology := range topologyCh {
			newTopology, err := ComputeTopology(topology.A, topology.B)
			if err != nil {
				log.Printf("failed to compute ps topology: %s", err)
				continue
			}

			outputCh <- newTopology
		}

		cancelFn()
		close(outputCh)
	}()

	return outputCh, nil
}
