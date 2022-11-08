package hooks

import (
	"context"
	"log"
	"sync"

	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"google.golang.org/grpc"
)

type HooksContext struct {
	lock     sync.Mutex
	counters map[string]*Counter
	barriers map[string]*Barrier
	hooks    map[string]*internal_hooks_v1.Hook
}

func newHooksContext() *HooksContext {
	return &HooksContext{
		counters: make(map[string]*Counter),
		barriers: make(map[string]*Barrier),
		hooks:    make(map[string]*internal_hooks_v1.Hook),
	}
}

// Gets a Counter by name, creating it if it does not exist.
func (i *HooksContext) GetCounter(name string) *Counter {
	i.lock.Lock()
	defer i.lock.Unlock()

	counter := i.counters[name]
	if counter == nil {
		counter = newCounter()
		i.counters[name] = counter
	}

	return counter
}

// Gets a Barrier by name, creating it if it does not exist.
func (i *HooksContext) GetBarrier(name string) *Barrier {
	i.lock.Lock()
	defer i.lock.Unlock()

	barrier := i.barriers[name]
	if barrier == nil {
		barrier = newBarrier()
		i.barriers[name] = barrier
	}

	return barrier
}

// TODO(brett19): This is called "AddHook" but technically is more like "SetHook"
func (i *HooksContext) AddHook(hook *internal_hooks_v1.Hook) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.hooks[hook.TargetMethod] = hook
}

func (i *HooksContext) HandleUnaryCall(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	hook := i.findHook(info.FullMethod)
	if hook == nil {
		// if there is no hook, we just run the default handler
		return handler(ctx, req)
	}

	log.Printf("calling registered hook: %+v", hook)
	rs := newRunState(i, handler, hook)
	return rs.Run(ctx, req)
}

func (i *HooksContext) findHook(methodName string) *internal_hooks_v1.Hook {
	i.lock.Lock()
	defer i.lock.Unlock()

	return i.hooks[methodName]
}
