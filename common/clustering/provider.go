package clustering

import "context"

/*
Note that the Join/Leave calls must not be called concurrently.  It is however
safe to concurrently call Join or Leave alongside Watch/Get calls.
*/
type Provider interface {
	Join(ctx context.Context, localConfig *Endpoint) error
	Leave(ctx context.Context) error

	Watch(ctx context.Context) (chan *Snapshot, error)
	Get(ctx context.Context) (*Snapshot, error)
}
