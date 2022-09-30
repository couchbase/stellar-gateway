package topology

import "github.com/couchbase/stellar-nebula/common/cbtopology"

type NsProviderOptions struct {
	LocalConfig Endpoint
	CbPoller    cbtopology.Provider
}

type NsProvider struct {
	cbPoller cbtopology.Provider
}

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
