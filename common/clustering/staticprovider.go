package clustering

import (
	"context"
	"sync"
)

type StaticProviderOptions struct {
}

type StaticProvider struct {
	localLock   sync.Mutex
	localConfig *Endpoint
}

func NewStaticProvider(opts StaticProviderOptions) (*StaticProvider, error) {
	return &StaticProvider{}, nil
}

var _ Provider = (*StaticProvider)(nil)

func (tp *StaticProvider) Join(ctx context.Context, localConfig *Endpoint) error {
	copiedConfig := *localConfig

	tp.localLock.Lock()
	if tp.localConfig != nil {
		tp.localLock.Unlock()
		return ErrAlreadyJoined
	}

	tp.localConfig = &copiedConfig
	tp.localLock.Unlock()

	return nil
}

func (tp *StaticProvider) Leave(ctx context.Context) error {
	tp.localLock.Lock()
	if tp.localConfig == nil {
		tp.localLock.Unlock()
		return ErrNotJoined
	}

	tp.localConfig = nil
	tp.localLock.Unlock()

	return nil
}

func (tp *StaticProvider) Watch(ctx context.Context) (chan *Snapshot, error) {
	// TODO(brett19): Implement pushing updates from Join/Leave calls.

	currentConfig, err := tp.Get(ctx)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Snapshot, 1)
	outputCh <- currentConfig

	return outputCh, nil
}

func (tp *StaticProvider) Get(ctx context.Context) (*Snapshot, error) {
	var localConfig *Endpoint

	tp.localLock.Lock()
	if tp.localConfig != nil {
		copiedConfig := *tp.localConfig
		localConfig = &copiedConfig
	}
	tp.localLock.Unlock()

	if localConfig == nil {
		return &Snapshot{
			Endpoints: nil,
		}, nil
	}

	return &Snapshot{
		RevEpoch: 0,
		Revision: 0,

		Endpoints: []Endpoint{
			*localConfig,
		},
	}, nil
}
