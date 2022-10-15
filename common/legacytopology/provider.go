package legacytopology

import "context"

type Provider interface {
	Watch(ctx context.Context, bucketName string) (<-chan *Topology, error)
}
