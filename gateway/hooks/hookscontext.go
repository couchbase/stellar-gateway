/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package hooks

import (
	"context"
	"sync"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type HooksContext struct {
	lock        sync.Mutex
	reqWatchers RequestWatchers
	counters    map[string]*Counter
	barriers    map[string]*Barrier
	hooks       map[string]*internal_hooks_v1.Hook
	logger      *zap.Logger
}

func newHooksContext(logger *zap.Logger) *HooksContext {
	return &HooksContext{
		reqWatchers: *newRequestWatchers(),
		counters:    make(map[string]*Counter),
		barriers:    make(map[string]*Barrier),
		hooks:       make(map[string]*internal_hooks_v1.Hook),
		logger:      logger,
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

func (i *HooksContext) WatchRequests(ctx context.Context) <-chan *RequestInfo {
	return i.reqWatchers.Watch(ctx)
}

func (i *HooksContext) SetHook(hook *internal_hooks_v1.Hook) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.hooks[hook.TargetMethod] = hook
}

func (i *HooksContext) dispatchReqToWatchers(
	ctx context.Context,
	fullMethod string,
	req interface{},
) {
	reqMsg := req.(proto.Message)
	reqAny, err := anypb.New(reqMsg)
	if err != nil {
		i.logger.Warn("request watcher failed to marshal request",
			zap.Error(err))
		return
	}

	md, _ := metadata.FromIncomingContext(ctx)

	reqInfo := &RequestInfo{
		MetaData:   md,
		FullMethod: fullMethod,
		Request:    reqAny,
	}

	i.reqWatchers.Send(reqInfo)
}

func (i *HooksContext) HandleUnaryCall(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	i.dispatchReqToWatchers(ctx, info.FullMethod, req)

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
