package hooks

import (
	"context"
	"log"
	"sync"

	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"google.golang.org/grpc"
)

type Interceptor struct {
	lock     sync.Mutex
	counters map[string]*Counter
	hooks    map[string]*internal_hooks_v1.Hook
}

func newInterceptor() *Interceptor {
	return &Interceptor{
		counters: make(map[string]*Counter),
		hooks:    make(map[string]*internal_hooks_v1.Hook),
	}
}

// Gets a Counter by name, creating it if it does not exist.
func (i *Interceptor) GetCounter(name string) *Counter {
	i.lock.Lock()
	defer i.lock.Unlock()

	counter := i.counters[name]
	if counter == nil {
		counter = newCounter()
		i.counters[name] = counter
	}

	return counter
}

// TODO(brett19): This is called "AddHook" but technically is more like "SetHook"
func (i *Interceptor) AddHook(hook *internal_hooks_v1.Hook) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.hooks[hook.TargetMethod] = hook
}

func (i *Interceptor) HandleUnaryCall(
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

func (i *Interceptor) findHook(methodName string) *internal_hooks_v1.Hook {
	i.lock.Lock()
	defer i.lock.Unlock()

	return i.hooks[methodName]
}
