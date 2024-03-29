package topology

import (
	"context"

	"github.com/couchbase/stellar-gateway/contrib/cbtopology"
	"github.com/couchbase/stellar-gateway/gateway/clustering"
	"github.com/couchbase/stellar-gateway/utils/channelmerge"
	"go.uber.org/zap"
)

type ManagerOptions struct {
	LocalTopologyProvider  clustering.Provider
	RemoteTopologyProvider cbtopology.Provider
	Logger                 *zap.Logger
}

type Manager struct {
	localTopologyProvider  clustering.Provider
	remoteTopologyProvider cbtopology.Provider
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

	remoteCh, err := m.remoteTopologyProvider.Watch(cancelCtx, bucketName)
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
