package topology

import (
	"context"

	"github.com/couchbase/stellar-gateway/client"
	"github.com/couchbase/stellar-gateway/legacybridge/clustering"
	"github.com/couchbase/stellar-gateway/utils/channelmerge"
	"go.uber.org/zap"
)

type RemoteTopologyProvider interface {
	WatchTopology(ctx context.Context, bucketName string) (<-chan *client.Topology, error)
}

var _ RemoteTopologyProvider = (*client.RoutingClient)(nil)

type ManagerOptions struct {
	LocalTopologyProvider  clustering.Provider
	RemoteTopologyProvider RemoteTopologyProvider
	Logger                 *zap.Logger
}

type Manager struct {
	localTopologyProvider  clustering.Provider
	remoteTopologyProvider RemoteTopologyProvider
	logger                 *zap.Logger
}

var _ Provider = (*Manager)(nil)

func NewManager(opts *ManagerOptions) (*Manager, error) {
	return &Manager{
		localTopologyProvider:  opts.LocalTopologyProvider,
		remoteTopologyProvider: opts.RemoteTopologyProvider,
		logger:                 opts.Logger,
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
				m.logger.Error("failed to compute ps topology", zap.Error(err))
				continue
			}

			outputCh <- newTopology
		}

		cancelFn()
		close(outputCh)
	}()

	return outputCh, nil
}
