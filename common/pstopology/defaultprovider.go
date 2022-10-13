package pstopology

import (
	"context"

	"github.com/couchbase/stellar-nebula/common/clustering"
	"github.com/couchbase/stellar-nebula/common/remotetopology"
)

type DefaultProviderOptions struct {
	ClusteringProvider     clustering.Provider
	RemoteTopologyProvider remotetopology.Provider
}

type DefaultProvider struct {
	clusteringProvider     clustering.Provider
	remoteTopologyProvider remotetopology.Provider
}

var _ Provider = (*DefaultProvider)(nil)

func NewDefaultProvider(opts *DefaultProviderOptions) (*DefaultProvider, error) {
	p := &DefaultProvider{
		clusteringProvider:     opts.ClusteringProvider,
		remoteTopologyProvider: opts.RemoteTopologyProvider,
	}

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *DefaultProvider) init() error {
	return nil
}

func (p *DefaultProvider) calcTopology(
	clusterSnapshot *clustering.Snapshot,
	remoteTopology *remotetopology.Topology,
) *Topology {
	// TODO(brett19): Implement topology generation
	return &Topology{}
}

func (p *DefaultProvider) watchCluster(ctx context.Context) (chan *Topology, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)

	clusterCh, err := p.clusteringProvider.Watch(cancelCtx)
	if err != nil {
		cancelFn()
		return nil, err
	}

	remoteCh, err := p.remoteTopologyProvider.WatchCluster(cancelCtx)
	if err != nil {
		cancelFn()
		return nil, err
	}

	outputCh := make(chan *Topology)

	go func() {
		// get the first copies of everything
		clusterSnapshot := <-clusterCh
		remoteTopology := <-remoteCh

	MainLoop:
		for {
			// dispatch the updated topology whenever something changes
			// TODO(brett19): We should probably implement some level of deduplication of topologies.
			// If one of the underlying topologies has changed but that is not represented in the
			// output topologies here, we will send unneccessary updates that could be avoided.
			topology := p.calcTopology(clusterSnapshot, remoteTopology)
			outputCh <- topology

			select {
			case newClusterSnapshot, ok := <-clusterCh:
				if !ok {
					break MainLoop
				}
				clusterSnapshot = newClusterSnapshot
			case newRemoteTopology, ok := <-remoteCh:
				if !ok {
					break MainLoop
				}
				remoteTopology = newRemoteTopology
			case <-ctx.Done():
				break MainLoop
			}
		}

		// close the output channel
		close(outputCh)

		// cancel the context to guarentee all the input channels close
		cancelFn()
	}()

	return outputCh, nil
}

func (p *DefaultProvider) watchBucket(ctx context.Context, bucketName string) (chan *Topology, error) {
	// TODO(brett19): Implement bucket-specific toplogies
	return p.watchCluster(ctx)
}

func (p *DefaultProvider) Watch(ctx context.Context, bucketName string) (chan *Topology, error) {
	if bucketName == "" {
		return p.watchCluster(ctx)
	} else {
		return p.watchBucket(ctx, bucketName)
	}
}
