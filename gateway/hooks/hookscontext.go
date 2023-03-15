package hooks

import (
	"context"
	"sync"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type HooksContext struct {
	lock     sync.Mutex
	counters map[string]*Counter
	barriers map[string]*Barrier
	hooks    map[string]*internal_hooks_v1.Hook
	logger   *zap.Logger
}

func newHooksContext(logger *zap.Logger) *HooksContext {
	return &HooksContext{
		counters: make(map[string]*Counter),
		barriers: make(map[string]*Barrier),
		hooks:    make(map[string]*internal_hooks_v1.Hook),
		logger: logger,
	}
}

func (i *HooksContext) getCounterLocked(name string) *Counter {
	counter := i.counters[name]
	if counter == nil {
		counter = newCounter()
		i.counters[name] = counter
	}

	return counter
}

// Gets a Counter by name, creating it if it does not exist.
func (i *HooksContext) GetCounter(name string) *Counter {
	i.lock.Lock()
	defer i.lock.Unlock()

	return i.getCounterLocked(name)
}

func (i *HooksContext) getBarrierLocked(name string) *Barrier {
	barrier := i.barriers[name]
	if barrier == nil {
		barrier = newBarrier(i.logger.Named("barrier"))
		i.barriers[name] = barrier
	}

	return barrier
}

// Gets a Barrier by name, creating it if it does not exist.
func (i *HooksContext) GetBarrier(name string) *Barrier {
	i.lock.Lock()
	defer i.lock.Unlock()

	return i.getBarrierLocked(name)
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

	i.logger.Info("calling registered hook: %+v", zap.Any("hook", hook))
	rs := newRunState(i, handler, hook, i.logger.Named("run-state"))
	return rs.Run(ctx, req)
}

func (i *HooksContext) findHook(methodName string) *internal_hooks_v1.Hook {
	i.lock.Lock()
	defer i.lock.Unlock()

	return i.hooks[methodName]
}

func (i *HooksContext) acquireRunLock(ctx context.Context) error {
	// TODO(brett19): This needs to watch ctx and return an error
	// if the context is cancelled before we manage to aquire the lock...
	i.lock.Lock()
	return nil
}

func (i *HooksContext) releaseRunLock() {
	i.lock.Unlock()
}
