package topologyutils

import (
	"context"

	"github.com/couchbase/stellar-nebula/common/nebclustering"
	"github.com/couchbase/stellar-nebula/common/remotetopology"
)

type MergedTopology struct {
	Local  *nebclustering.Snapshot
	Remote *remotetopology.Topology
}

func TopologyMergeWatch(
	ctx context.Context,
	localTopologyProvider nebclustering.Provider,
	remoteTopologyProvider remotetopology.Provider,
	bucketName string,
) (<-chan MergedTopology, error) {
	cancelCtx, cancelFn := context.WithCancel(ctx)

	clusterCh, err := localTopologyProvider.Watch(cancelCtx)
	if err != nil {
		cancelFn()
		return nil, err
	}

	remoteCh, err := remoteTopologyProvider.Watch(cancelCtx, bucketName)
	if err != nil {
		cancelFn()
		return nil, err
	}

	outputCh := make(chan MergedTopology)

	go func() {
		// get the first copies of everything
		clusterSnapshot := <-clusterCh
		remoteTopology := <-remoteCh

	MainLoop:
		for {
			outputCh <- MergedTopology{
				Local:  clusterSnapshot,
				Remote: remoteTopology,
			}

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
