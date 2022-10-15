package nebclustering

import "context"

type Provider interface {
	Watch(ctx context.Context) (chan *Snapshot, error)
}
