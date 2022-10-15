package remotetopology

import "context"

type Provider interface {
	WatchCluster(ctx context.Context) (<-chan *Topology, error)
	WatchBucket(ctx context.Context, bucketName string) (<-chan *Topology, error)
}
