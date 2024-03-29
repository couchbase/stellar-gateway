package hooks

import (
	"context"
	"sync"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/anypb"
)

type RequestInfo struct {
	MetaData   map[string][]string
	FullMethod string
	Request    *anypb.Any
}

type requestWatcher struct {
	Ctx context.Context
	Ch  chan<- *RequestInfo
}

type RequestWatchers struct {
	lock     sync.Mutex
	watchers []*requestWatcher
}

func newRequestWatchers() *RequestWatchers {
	return &RequestWatchers{}
}

func (r *RequestWatchers) Send(req *RequestInfo) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, watcher := range r.watchers {
		select {
		case watcher.Ch <- req:
		case <-watcher.Ctx.Done():
		}
	}
}

func (r *RequestWatchers) Watch(ctx context.Context) <-chan *RequestInfo {
	outputCh := make(chan *RequestInfo)

	go func() {
		watcher := &requestWatcher{
			Ctx: ctx,
			Ch:  outputCh,
		}

		r.lock.Lock()
		r.watchers = append(r.watchers, watcher)
		r.lock.Unlock()

		<-ctx.Done()

		r.lock.Lock()
		watcherIdx := slices.Index(r.watchers, watcher)
		if watcherIdx >= 0 {
			r.watchers[watcherIdx] = r.watchers[len(r.watchers)-1]
			r.watchers = r.watchers[:len(r.watchers)-1]
		}
		r.lock.Unlock()

		close(outputCh)
	}()

	return outputCh
}
