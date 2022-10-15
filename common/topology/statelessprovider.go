package topology

import (
	"context"

	"github.com/couchbase/stellar-nebula/common/nebclustering"
	"github.com/couchbase/stellar-nebula/common/remotetopology"
	"github.com/couchbase/stellar-nebula/contrib/revisionarr"
)

type StatelessProviderOptions struct {
	ClusteringManager      *nebclustering.Manager
	RemoteTopologyProvider remotetopology.Provider
}

type StatelessProvider struct {
	clusteringManager      *nebclustering.Manager
	remoteTopologyProvider remotetopology.Provider
}

var _ Provider = (*StatelessProvider)(nil)

func NewStatelessProvider(opts *StatelessProviderOptions) (*StatelessProvider, error) {
	p := &StatelessProvider{
		clusteringManager:      opts.ClusteringManager,
		remoteTopologyProvider: opts.RemoteTopologyProvider,
	}

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *StatelessProvider) init() error {
	return nil
}

func (p *StatelessProvider) calcTopology(
	clusterSnapshot *nebclustering.Snapshot,
	remoteTopology *remotetopology.Topology,
) *Topology {
	// The local topology and remote topology revisions must be monotonically increasing which
	// means that we can safely calculate the derived revision through addition.
	mergedRevision := revisionarr.Add(clusterSnapshot.Revision, remoteTopology.Revision)

	return &Topology{
		Revision:       mergedRevision,
		LocalTopology:  clusterSnapshot,
		RemoteTopology: remoteTopology,
	}
}

func (p *StatelessProvider) Watch(ctx context.Context, bucketName string) (chan *Topology, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)

	clusterCh, err := p.clusteringManager.Watch(cancelCtx)
	if err != nil {
		cancelFn()
		return nil, err
	}

	remoteCh, err := p.remoteTopologyProvider.Watch(cancelCtx, bucketName)
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
