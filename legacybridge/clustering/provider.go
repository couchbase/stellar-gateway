package clustering

import "context"

type Provider interface {
	Watch(ctx context.Context) (chan *Snapshot, error)
}
