package clustering

import (
	"context"

	"github.com/couchbase/stellar-nebula/common/cbtopology"
)

type NsProviderOptions struct {
	LocalConfig Endpoint
	CbPoller    cbtopology.Provider
}

type NsProvider struct {
	cbPoller cbtopology.Provider
}

var _ Provider = (*NsProvider)(nil)

func NewNsProvider(opts NsProviderOptions) (*NsProvider, error) {
	p := &NsProvider{
		cbPoller: opts.CbPoller,
	}

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (t *NsProvider) init() error {
	// TODO(brett19): Need to implement NsProvider
	panic("not yet implemented")
}

func (tp *NsProvider) Join(ctx context.Context, localConfig *Endpoint) error {
	panic("not yet implemented")
}

func (tp *NsProvider) Leave(ctx context.Context) error {
	panic("not yet implemented")
}

func (tp *NsProvider) Watch(ctx context.Context) (chan *Snapshot, error) {
	panic("not yet implemented")
}

func (tp *NsProvider) Get(ctx context.Context) (*Snapshot, error) {
	panic("not yet implemented")
}
