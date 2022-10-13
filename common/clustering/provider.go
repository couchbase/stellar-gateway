package clustering

import "context"

type Membership interface {
	UpdateMetaData(ctx context.Context, metaData []byte) error
	Leave(ctx context.Context) error
}

/*
Note that the Join/Leave calls must not be called concurrently.  It is however
safe to concurrently call Join or Leave alongside Watch/Get calls.
*/
type Provider interface {
	Join(ctx context.Context, memberID string, metaData []byte) (Membership, error)

	Watch(ctx context.Context) (chan *Snapshot, error)
	Get(ctx context.Context) (*Snapshot, error)
}
